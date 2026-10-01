package live

import java.nio.file.Files
import java.nio.file.NoSuchFileException
import java.nio.file.Path
import java.nio.file.attribute.BasicFileAttributes
import java.util.UUID
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest
import software.amazon.dax.*

private fun string(value: kotlin.String) = DaxAttributeValue.String(value)
private fun matchesItem(
    item: Map<kotlin.String, DaxAttributeValue>?,
    key: Map<kotlin.String, DaxAttributeValue>,
    value: kotlin.String,
) = item != null && (key + ("value" to string(value))).all { (name, expected) -> item[name] == expected }

private fun verifyDynamoDbItems(region: kotlin.String, table: kotlin.String, partition: kotlin.String) {
    DynamoDbClient.builder().region(Region.of(region)).build().use { dynamoDb ->
        for (sortKey in listOf("BATCH#1", "BATCH#2")) {
            val item = dynamoDb.getItem(GetItemRequest.builder()
                .tableName(table)
                .consistentRead(true)
                .key(mapOf(
                    "PK" to AttributeValue.builder().s(partition).build(),
                    "SK" to AttributeValue.builder().s(sortKey).build(),
                ))
                .build())
                .item()
            check(item["PK"]?.s() == partition && item["SK"]?.s() == sortKey && item["value"]?.s() == "batch") {
                "DynamoDB did not return expected item PK=$partition SK=$sortKey"
            }
        }
    }
}

private suspend fun faultBarrier(client: DaxClient) {
    val ready = System.getenv("DAX_FAULT_READY_FILE")
    val release = System.getenv("DAX_FAULT_RELEASE_FILE")
    if (ready.isNullOrEmpty() || release.isNullOrEmpty()) return
    check(client.discoverEndpoints().isNotEmpty()) { "fault prewarm discovered no endpoints" }
    client.refreshEndpoints()
    check(client.routeSnapshot().isNotEmpty()) { "fault prewarm has no routes" }
    Files.createFile(Path.of(ready))
    val start = System.nanoTime()
    while (System.nanoTime() - start < 120_000_000_000L) {
        try {
            check(Files.readAttributes(Path.of(release), BasicFileAttributes::class.java).isRegularFile) {
                "fault release path is not a regular file"
            }
            return
        } catch (missing: NoSuchFileException) { }
        delay(100)
    }
    error("timed out waiting for DAX_FAULT_RELEASE_FILE after 120 seconds")
}

fun main() = runBlocking {
    val endpoint = requireNotNull(System.getenv("DAX_ENDPOINT")) { "DAX_ENDPOINT is required" }
    val region = System.getenv("AWS_REGION") ?: "eu-west-1"
    val table = System.getenv("DAX_TABLE_NAME") ?: "DaxParityTable"
    val tableCleanupEnabled = !System.getenv("DAX_FFI_TABLE_CLEANUP").equals("false", ignoreCase = true)
    val config = DaxClientConfig(
        listOf(endpoint), region, 60_000uL, 2u, 2u, 0uL, 70uL, 20_000uL, 10,
        4_000uL, 125uL, 30_000uL, 5_000uL, false, false,
        DaxIpDiscovery.DEFAULT, DaxLogLevel.OFF,
    )
    val client = createDaxClient(config)
    try {
        faultBarrier(client)
    } catch (error: Throwable) {
        client.shutdown()
        client.close()
        throw error
    }
    val partition = "FFI#KOTLIN#${UUID.randomUUID()}"
    val key = mapOf("PK" to string(partition), "SK" to string("ITEM#1"))
    val batchKeys = listOf("BATCH#1", "BATCH#2").map { sortKey ->
        mapOf("PK" to string(partition), "SK" to string(sortKey))
    }
    val transactionKeys = listOf("TRANSACTION#1", "TRANSACTION#2").map { sortKey ->
        mapOf("PK" to string(partition), "SK" to string(sortKey))
    }
    val postShutdownPaginator = client.queryPaginator(DaxQueryRequest(
        table, null, "PK = :pk", null, null, emptyMap(),
        mapOf(":pk" to string(partition)), null, 1, null, null, null, null,
    ))
    var inserted = false
    try {
        client.putItem(DaxPutItemRequest(
            table, key + ("value" to string("created")), null, null, emptyMap(), emptyMap(),
        ))
        inserted = true

        var observed = false
        for (attempt in 0 until 20) {
            if (client.getItem(DaxGetItemRequest(
                table, key, null, null, emptyMap(),
            )).item != null) {
                observed = true
                break
            }
            delay(100)
        }
        check(observed) { "GetItem did not observe the inserted item" }

        val updated = client.updateItem(DaxUpdateItemRequest(
            table, key, "SET #value = :value", null,
            mapOf("#value" to "value"), mapOf(":value" to string("updated")),
            DaxReturnValue.ALL_OLD,
        ))
        check(updated.attributes != null) { "UpdateItem did not return ALL_OLD" }

        val query = client.query(DaxQueryRequest(
            table, null, "PK = :pk", null, null, emptyMap(),
            mapOf(":pk" to string(partition)), null, 10, null, null, null, null,
        ))
        check(query.items.isNotEmpty()) { "Query returned no items" }

        val scan = client.scan(DaxScanRequest(
            table, null, "PK = :pk", null, emptyMap(),
            mapOf(":pk" to string(partition)), null, null, null, null, null,
        ))
        check(scan.items.isNotEmpty()) { "Scan returned no items" }

        val batchWrite = client.batchWriteItem(DaxBatchWriteItemRequest(
            mapOf(table to batchKeys.map { DaxBatchWriteAction.Put(it + ("value" to string("batch"))) }),
            DaxReturnConsumedCapacity.TOTAL, null,
        ))
        check(batchWrite.unprocessedItems.isEmpty()) { "BatchWriteItem put returned unprocessed items" }
        val batchGet = client.batchGetItem(DaxBatchGetItemRequest(
            mapOf(table to DaxKeysAndAttributes(batchKeys, null, null, emptyMap())),
            DaxReturnConsumedCapacity.TOTAL,
        ))
        val batchItems = batchGet.responses[table]
        check(batchItems != null && batchItems.size == 2) { "BatchGetItem did not return both items" }
        check(batchGet.unprocessedKeys.isEmpty()) { "BatchGetItem returned unprocessed keys" }
        check(batchKeys.all { batchKey -> batchItems.any { matchesItem(it, batchKey, "batch") } }) {
            "BatchGetItem returned an unexpected item"
        }
        verifyDynamoDbItems(region, table, partition)
        for (stopOnDuplicateToken in listOf(false, true)) {
            client.batchGetItemPaginator(DaxBatchGetItemRequest(
                mapOf(table to DaxKeysAndAttributes(batchKeys, null, null, emptyMap())), null,
            ), stopOnDuplicateToken).use { paginator ->
                val page = paginator.nextPage()
                check(page != null && page.responses[table]?.size == batchKeys.size) {
                    "BatchGet paginator did not return its terminal page"
                }
                check(!paginator.hasMorePages()) {
                    "BatchGet paginator retained a continuation without unprocessed keys"
                }
                check(paginator.nextPage() == null) {
                    "BatchGet paginator returned a page after its terminal token"
                }
            }
        }
        client.queryPaginator(DaxQueryRequest(
            table, null, "PK = :pk", null, null, emptyMap(),
            mapOf(":pk" to string(partition)), null, 1, null, null, null, null,
        )).use { paginator ->
            var pages = 0
            while (paginator.hasMorePages()) {
                if (paginator.nextPage() == null) break
                pages++
            }
            check(pages >= 2) { "Query paginator did not traverse at least two pages" }
        }
        client.scanPaginator(DaxScanRequest(
            table, null, "PK = :pk", null, emptyMap(),
            mapOf(":pk" to string(partition)), null, 1, null, null, null,
        )).use { paginator ->
            var pages = 0
            while (paginator.hasMorePages() && pages < 100) {
                if (paginator.nextPage() == null) break
                pages++
            }
            check(pages >= 2) { "Scan paginator did not traverse at least two pages" }
        }
        val discovered = client.discoverEndpoints()
        check(discovered.isNotEmpty()) { "endpoint discovery returned no endpoints" }
        check(!client.activeEndpoints().isNullOrEmpty()) {
            "active endpoints were not committed by discovery"
        }
        client.refreshEndpoints()
        check(client.routeSnapshot().isNotEmpty()) { "route snapshot was empty after endpoint refresh" }
        if (tableCleanupEnabled) {
            val batchDelete = client.batchWriteItem(DaxBatchWriteItemRequest(
                mapOf(table to batchKeys.map { DaxBatchWriteAction.Delete(it) }), null, null,
            ))
            check(batchDelete.unprocessedItems.isEmpty()) { "BatchWriteItem delete returned unprocessed items" }
        }

        client.transactWriteItems(DaxTransactWriteItemsRequest(
            transactionKeys.map { DaxTransactWriteAction.Put(
                table, it + ("value" to string("transaction")), null, emptyMap(), emptyMap(), null,
            ) }, null, DaxReturnConsumedCapacity.TOTAL, null,
        ))
        val transactionGet = client.transactGetItems(DaxTransactGetItemsRequest(
            transactionKeys.map { DaxTransactGetAction(table, it, null, emptyMap()) },
            DaxReturnConsumedCapacity.TOTAL,
        ))
        check(transactionGet.responses.size == 2) { "TransactGetItems did not return both items" }
        transactionKeys.forEachIndexed { i, transactionKey ->
            check(matchesItem(transactionGet.responses[i].item, transactionKey, "transaction")) {
                "TransactGetItems returned an unexpected item"
            }
        }

        try {
            client.transactWriteItems(DaxTransactWriteItemsRequest(
                listOf(DaxTransactWriteAction.ConditionCheck(
                    table, transactionKeys[0], "attribute_not_exists(PK)", emptyMap(), emptyMap(),
                    DaxReturnValuesOnConditionCheckFailure.ALL_OLD,
                )), null, null, null,
            ))
            error("failing transaction unexpectedly succeeded")
        } catch (error: DaxFfiException.Failure) {
            check(error.details.category == DaxErrorCategory.SERVICE) { "failing transaction was not a service error" }
            check(error.details.cancellationReasons.isNotEmpty()) { "failing transaction did not return cancellation reasons" }
        }
        if (tableCleanupEnabled) {
            client.transactWriteItems(DaxTransactWriteItemsRequest(
                transactionKeys.map { DaxTransactWriteAction.Delete(
                    table, it, null, emptyMap(), emptyMap(), null,
                ) }, null, null, null,
            ))
        }
    } finally {
        try {
            if (tableCleanupEnabled) {
                withContext(NonCancellable) {
                    var cleanupFailure: Exception? = null
                    for (cleanupKey in batchKeys + transactionKeys + listOf(key)) {
                        try {
                            val deleted = client.deleteItem(DaxDeleteItemRequest(
                                table, cleanupKey, if (cleanupKey === key) DaxReturnValue.ALL_OLD else null,
                                null, emptyMap(), emptyMap(),
                            ))
                            if (cleanupKey === key && inserted) {
                                check(deleted.attributes != null) { "DeleteItem did not return ALL_OLD" }
                            }
                        } catch (error: Exception) {
                            val previous = cleanupFailure
                            if (previous == null) cleanupFailure = error else previous.addSuppressed(error)
                        }
                    }
                    cleanupFailure?.let { throw it }
                }
            }
        } finally {
            try {
                client.shutdown()
                check(client.isClosed()) { "client was not closed after shutdown" }
                try {
                    postShutdownPaginator.nextPage()
                    error("post-shutdown paginator call unexpectedly succeeded")
                } catch (_: DaxFfiException.Failure) {
                }
                try {
                    client.getItem(DaxGetItemRequest(table, key, null, null, emptyMap()))
                    error("post-shutdown operation unexpectedly succeeded")
                } catch (_: DaxFfiException.Failure) {
                }
            } finally {
                postShutdownPaginator.close()
                client.close()
            }
        }
    }
    if (!tableCleanupEnabled) println("Kotlin FFI retained test items PK=$partition")
    println("Kotlin FFI direct, batch, transaction, paginator, route, and lifecycle operations passed")
}
