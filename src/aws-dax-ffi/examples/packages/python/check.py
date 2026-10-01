import asyncio
import os
import uuid
import aws_dax_ffi as dax

if not dax.ffi_contract_version():
    raise RuntimeError("empty FFI contract version")
live = os.getenv("DAX_FFI_LIVE") == "1"
config = dax.DaxClientConfig(
    endpoints=["127.0.0.1:8111"], region=os.getenv("AWS_REGION"), request_timeout_millis=60_000,
    write_retries=2, read_retries=2, retry_delay_millis=0,
    throttle_base_delay_millis=70, throttle_max_backoff_millis=20_000,
    max_pending_connections_per_host=10, cluster_update_interval_millis=4_000,
    cluster_update_threshold_millis=125, idle_connection_reap_delay_millis=30_000,
    client_health_check_interval_millis=5_000, skip_hostname_verification=False,
    route_manager_enabled=False, ip_discovery=dax.DaxIpDiscovery.DEFAULT,
    log_level=dax.DaxLogLevel.OFF,
)


async def live_operations(client):
    table = os.getenv("DAX_TABLE_NAME") or "DaxParityTable"
    run_id = uuid.uuid4().hex
    key = {
        "PK": dax.DaxAttributeValue.STRING(f"PACKAGE#PYTHON#{run_id}"),
        "SK": dax.DaxAttributeValue.STRING(f"ITEM#{run_id}"),
    }
    item = {**key, "value": dax.DaxAttributeValue.STRING("created")}
    inserted = False
    try:
        await client.put_item(dax.DaxPutItemRequest(
            table_name=table, item=item, return_values=None, condition_expression=None,
            expression_attribute_names={}, expression_attribute_values={},
        ))
        inserted = True
        for attempt in range(20):
            output = await client.get_item(dax.DaxGetItemRequest(
                table_name=table, key=key, consistent_read=None,
                projection_expression=None, expression_attribute_names={},
            ))
            if output.item is not None:
                if output.item != item:
                    raise RuntimeError("GetItem did not match the inserted item")
                break
            if attempt < 19:
                await asyncio.sleep(0.1)
        else:
            raise RuntimeError("GetItem did not observe the inserted item after 20 attempts")
    finally:
        # A failed PutItem response can still mean a write.
        output = await client.delete_item(dax.DaxDeleteItemRequest(
            table_name=table, key=key, return_values=dax.DaxReturnValue.ALL_OLD,
            condition_expression=None, expression_attribute_names={}, expression_attribute_values={},
        ))
        if inserted and output.attributes != item:
            raise RuntimeError("DeleteItem ALL_OLD did not match the inserted item")


async def main():
    if live or os.getenv("DAX_FFI_CREATE_CLIENT") == "1":
        endpoint = os.getenv("DAX_ENDPOINT")
        if not endpoint:
            raise RuntimeError("DAX_ENDPOINT is required")
        config.endpoints = [endpoint]
        client = await dax.create_dax_client(config)
        try:
            if live:
                await live_operations(client)
        finally:
            try:
                client.shutdown()
            finally:
                del client
    print("Python package live operations passed" if live else "python package native load ok")


if __name__ == "__main__":
    asyncio.run(main())
