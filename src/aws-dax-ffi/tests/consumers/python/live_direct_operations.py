import asyncio
import os
import stat
import time

import aws_dax_ffi as dax
import boto3


def string(value):
    return dax.DaxAttributeValue.STRING(value)


def config():
    return dax.DaxClientConfig(
        endpoints=[os.environ["DAX_ENDPOINT"]],
        region=os.environ.get("AWS_REGION", "eu-west-1"),
        request_timeout_millis=60_000,
        write_retries=2,
        read_retries=2,
        retry_delay_millis=0,
        throttle_base_delay_millis=70,
        throttle_max_backoff_millis=20_000,
        max_pending_connections_per_host=10,
        cluster_update_interval_millis=4_000,
        cluster_update_threshold_millis=125,
        idle_connection_reap_delay_millis=30_000,
        client_health_check_interval_millis=5_000,
        skip_hostname_verification=False,
        route_manager_enabled=False,
        ip_discovery=dax.DaxIpDiscovery.DEFAULT,
        log_level=dax.DaxLogLevel.OFF,
    )


def verify_dynamodb_items(region, table, partition):
    dynamodb = boto3.client("dynamodb", region_name=region)
    for sort_key in ("BATCH#1", "BATCH#2"):
        response = dynamodb.get_item(
            TableName=table,
            ConsistentRead=True,
            Key={"PK": {"S": partition}, "SK": {"S": sort_key}},
        )
        item = response.get("Item", {})
        if item.get("PK", {}).get("S") != partition or item.get("SK", {}).get("S") != sort_key or \
                item.get("value", {}).get("S") != "batch":
            raise AssertionError(f"DynamoDB did not return expected item PK={partition} SK={sort_key}")


async def fault_barrier(client):
    ready = os.environ.get("DAX_FAULT_READY_FILE")
    release = os.environ.get("DAX_FAULT_RELEASE_FILE")
    if not ready or not release:
        return
    if not await client.discover_endpoints():
        raise RuntimeError("fault prewarm discovered no endpoints")
    await client.refresh_endpoints()
    if not client.route_snapshot():
        raise RuntimeError("fault prewarm has no routes")
    with open(ready, "x"):
        pass
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        try:
            if not stat.S_ISREG(os.stat(release).st_mode):
                raise RuntimeError("fault release path is not a regular file")
            return
        except FileNotFoundError:
            await asyncio.sleep(0.1)
    raise TimeoutError("timed out waiting for DAX_FAULT_RELEASE_FILE after 120 seconds")


async def main():
    region = os.environ.get("AWS_REGION", "eu-west-1")
    table = os.environ.get("DAX_TABLE_NAME", "DaxParityTable")
    table_cleanup_enabled = os.environ.get("DAX_FFI_TABLE_CLEANUP", "true").lower() != "false"
    partition = f"FFI#PYTHON#{time.time_ns()}"
    key = {"PK": string(partition), "SK": string("ITEM#1")}
    batch_keys = [
        {"PK": string(partition), "SK": string("BATCH#1")},
        {"PK": string(partition), "SK": string("BATCH#2")},
    ]
    transaction_keys = [
        {"PK": string(partition), "SK": string("TRANSACTION#1")},
        {"PK": string(partition), "SK": string("TRANSACTION#2")},
    ]
    client = await dax.create_dax_client(config())
    try:
        await fault_barrier(client)
    except BaseException:
        client.shutdown()
        raise
    post_shutdown_paginator = client.query_paginator(
        dax.DaxQueryRequest(
            table_name=table, index_name=None, key_condition_expression="PK = :pk",
            filter_expression=None, projection_expression=None, expression_attribute_names={},
            expression_attribute_values={":pk": string(partition)}, consistent_read=None,
            limit=1, scan_index_forward=None, select=None, return_consumed_capacity=None,
            exclusive_start_key=None,
        )
    )
    inserted = False
    try:
        await client.put_item(
            dax.DaxPutItemRequest(
                table_name=table,
                item={**key, "value": string("created")},
                return_values=None,
                condition_expression=None,
                expression_attribute_names={},
                expression_attribute_values={},
            )
        )
        inserted = True
        for _ in range(20):
            output = await client.get_item(
                dax.DaxGetItemRequest(
                    table_name=table,
                    key=key,
                    consistent_read=None,
                    projection_expression=None,
                    expression_attribute_names={},
                )
            )
            if output.item is not None:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("GetItem did not observe the inserted item")

        updated = await client.update_item(
            dax.DaxUpdateItemRequest(
                table_name=table,
                key=key,
                update_expression="SET #value = :value",
                condition_expression=None,
                expression_attribute_names={"#value": "value"},
                expression_attribute_values={":value": string("updated")},
                return_values=dax.DaxReturnValue.ALL_OLD,
            )
        )
        assert updated.attributes is not None

        query = await client.query(
            dax.DaxQueryRequest(
                table_name=table,
                index_name=None,
                key_condition_expression="PK = :pk",
                filter_expression=None,
                projection_expression=None,
                expression_attribute_names={},
                expression_attribute_values={":pk": string(partition)},
                consistent_read=None,
                limit=10,
                scan_index_forward=None,
                select=None,
                return_consumed_capacity=None,
                exclusive_start_key=None,
            )
        )
        assert query.items

        scan = await client.scan(
            dax.DaxScanRequest(
                table_name=table,
                index_name=None,
                filter_expression="PK = :pk",
                projection_expression=None,
                expression_attribute_names={},
                expression_attribute_values={":pk": string(partition)},
                consistent_read=None,
                limit=None,
                select=None,
                return_consumed_capacity=None,
                exclusive_start_key=None,
            )
        )
        assert scan.items

        batch_write = await client.batch_write_item(
            dax.DaxBatchWriteItemRequest(
                request_items={
                    table: [
                        dax.DaxBatchWriteAction.PUT(
                            {**batch_key, "value": string("batch")}
                        )
                        for batch_key in batch_keys
                    ]
                },
                return_consumed_capacity=dax.DaxReturnConsumedCapacity.TOTAL,
                return_item_collection_metrics=None,
            )
        )
        assert not batch_write.unprocessed_items

        batch_get = await client.batch_get_item(
            dax.DaxBatchGetItemRequest(
                request_items={
                    table: dax.DaxKeysAndAttributes(
                        keys=batch_keys,
                        consistent_read=None,
                        projection_expression=None,
                        expression_attribute_names={},
                    )
                },
                return_consumed_capacity=dax.DaxReturnConsumedCapacity.TOTAL,
            )
        )
        assert len(batch_get.responses[table]) == 2
        assert not batch_get.unprocessed_keys
        verify_dynamodb_items(region, table, partition)
        for stop_on_duplicate_token in (False, True):
            paginator = client.batch_get_item_paginator(
                dax.DaxBatchGetItemRequest(
                    request_items={
                        table: dax.DaxKeysAndAttributes(
                            keys=batch_keys, consistent_read=None,
                            projection_expression=None, expression_attribute_names={},
                        )
                    },
                    return_consumed_capacity=None,
                ),
                stop_on_duplicate_token,
            )
            page = await paginator.next_page()
            assert page is not None and len(page.responses[table]) == len(batch_keys)
            assert not paginator.has_more_pages()
            assert await paginator.next_page() is None
        paginator = client.query_paginator(
            dax.DaxQueryRequest(
                table_name=table, index_name=None, key_condition_expression="PK = :pk",
                filter_expression=None, projection_expression=None, expression_attribute_names={},
                expression_attribute_values={":pk": string(partition)}, consistent_read=None,
                limit=1, scan_index_forward=None, select=None, return_consumed_capacity=None,
                exclusive_start_key=None,
            )
        )
        pages = 0
        while paginator.has_more_pages():
            if await paginator.next_page() is None:
                break
            pages += 1
        assert pages >= 2
        paginator = client.scan_paginator(
            dax.DaxScanRequest(
                table_name=table, index_name=None, filter_expression="PK = :pk",
                projection_expression=None, expression_attribute_names={},
                expression_attribute_values={":pk": string(partition)}, consistent_read=None,
                limit=1, select=None, return_consumed_capacity=None, exclusive_start_key=None,
            )
        )
        pages = 0
        while paginator.has_more_pages() and pages < 100:
            if await paginator.next_page() is None:
                break
            pages += 1
        assert pages >= 2
        discovered = await client.discover_endpoints()
        assert discovered
        assert client.active_endpoints()
        await client.refresh_endpoints()
        assert client.route_snapshot()

        if table_cleanup_enabled:
            batch_delete = await client.batch_write_item(
                dax.DaxBatchWriteItemRequest(
                    request_items={
                        table: [
                            dax.DaxBatchWriteAction.DELETE(batch_key)
                            for batch_key in batch_keys
                        ]
                    },
                    return_consumed_capacity=None,
                    return_item_collection_metrics=None,
                )
            )
            assert not batch_delete.unprocessed_items

        await client.transact_write_items(
            dax.DaxTransactWriteItemsRequest(
                transact_items=[
                    dax.DaxTransactWriteAction.PUT(
                        table,
                        {**transaction_key, "value": string("transaction")},
                        None,
                        {},
                        {},
                        None,
                    )
                    for transaction_key in transaction_keys
                ],
                client_request_token=None,
                return_consumed_capacity=dax.DaxReturnConsumedCapacity.TOTAL,
                return_item_collection_metrics=None,
            )
        )

        transaction_get = await client.transact_get_items(
            dax.DaxTransactGetItemsRequest(
                transact_items=[
                    dax.DaxTransactGetAction(
                        table_name=table,
                        key=transaction_key,
                        projection_expression=None,
                        expression_attribute_names={},
                    )
                    for transaction_key in transaction_keys
                ],
                return_consumed_capacity=dax.DaxReturnConsumedCapacity.TOTAL,
            )
        )
        assert len(transaction_get.responses) == 2
        assert all(response.item is not None for response in transaction_get.responses)

        try:
            await client.transact_write_items(
                dax.DaxTransactWriteItemsRequest(
                    transact_items=[
                        dax.DaxTransactWriteAction.CONDITION_CHECK(
                            table,
                            transaction_keys[0],
                            "attribute_not_exists(PK)",
                            {},
                            {},
                            dax.DaxReturnValuesOnConditionCheckFailure.ALL_OLD,
                        )
                    ],
                    client_request_token=None,
                    return_consumed_capacity=None,
                    return_item_collection_metrics=None,
                )
            )
            raise AssertionError("failing transaction unexpectedly succeeded")
        except dax.DaxFfiError.Failure as error:
            assert error.details.category == dax.DaxErrorCategory.SERVICE
            assert error.details.cancellation_reasons

        if table_cleanup_enabled:
            await client.transact_write_items(
                dax.DaxTransactWriteItemsRequest(
                    transact_items=[
                        dax.DaxTransactWriteAction.DELETE(
                            table,
                            transaction_key,
                            None,
                            {},
                            {},
                            None,
                        )
                        for transaction_key in transaction_keys
                    ],
                    client_request_token=None,
                    return_consumed_capacity=None,
                    return_item_collection_metrics=None,
                )
            )
    finally:
        if table_cleanup_enabled:
            for cleanup_key in [*batch_keys, *transaction_keys]:
                await client.delete_item(
                    dax.DaxDeleteItemRequest(
                        table_name=table,
                        key=cleanup_key,
                        return_values=None,
                        condition_expression=None,
                        expression_attribute_names={},
                        expression_attribute_values={},
                    )
                )
            if inserted:
                deleted = await client.delete_item(
                    dax.DaxDeleteItemRequest(
                        table_name=table,
                        key=key,
                        return_values=dax.DaxReturnValue.ALL_OLD,
                        condition_expression=None,
                        expression_attribute_names={},
                        expression_attribute_values={},
                    )
                )
                assert deleted.attributes is not None
        client.shutdown()
    assert client.is_closed()
    try:
        await post_shutdown_paginator.next_page()
        raise AssertionError("post-shutdown paginator call unexpectedly succeeded")
    except dax.DaxFfiError.Failure:
        pass
    try:
        await client.get_item(dax.DaxGetItemRequest(
            table_name=table, key=key, consistent_read=None,
            projection_expression=None, expression_attribute_names={},
        ))
        raise AssertionError("post-shutdown operation unexpectedly succeeded")
    except dax.DaxFfiError.Failure:
        pass
    if not table_cleanup_enabled:
        print(f"Python FFI retained test items PK={partition}")
    print("Python FFI direct, batch, transaction, paginator, route, and lifecycle operations passed")


if __name__ == "__main__":
    asyncio.run(main())
