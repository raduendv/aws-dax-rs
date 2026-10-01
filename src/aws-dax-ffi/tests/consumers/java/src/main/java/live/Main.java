package live;

import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.stream.Stream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.dax.*;

public final class Main {
    private static void faultBarrier(DaxClient client) throws Exception {
        var ready = System.getenv("DAX_FAULT_READY_FILE");
        var release = System.getenv("DAX_FAULT_RELEASE_FILE");
        if (ready == null || ready.isEmpty() || release == null || release.isEmpty()) return;
        check(!client.discoverEndpoints().join().isEmpty(), "fault prewarm discovered no endpoints");
        client.refreshEndpoints().join();
        check(!client.routeSnapshot().isEmpty(), "fault prewarm has no routes");
        Files.createFile(Path.of(ready));
        var start = System.nanoTime();
        while (System.nanoTime() - start < 120_000_000_000L) {
            try {
                check(Files.readAttributes(Path.of(release), BasicFileAttributes.class).isRegularFile(),
                    "fault release path is not a regular file");
                return;
            } catch (NoSuchFileException missing) { }
            Thread.sleep(100);
        }
        throw new java.util.concurrent.TimeoutException(
            "timed out waiting for DAX_FAULT_RELEASE_FILE after 120 seconds");
    }

    private static DaxAttributeValue string(String value) {
        return new DaxAttributeValue.String(value);
    }

    private static Map<String, DaxAttributeValue> itemWithValue(Map<String, DaxAttributeValue> key, String value) {
        var item = new HashMap<>(key);
        item.put("value", string(value));
        return item;
    }

    private static boolean matchesItem(Map<String, DaxAttributeValue> item,
            Map<String, DaxAttributeValue> key, String value) {
        return item != null && item.entrySet().containsAll(itemWithValue(key, value).entrySet());
    }

    private static void verifyDynamoDbItems(String region, String table, String partition) {
        try (var dynamoDb = DynamoDbClient.builder().region(Region.of(region)).build()) {
            for (var sortKey : List.of("BATCH#1", "BATCH#2")) {
                var response = dynamoDb.getItem(GetItemRequest.builder()
                    .tableName(table)
                    .consistentRead(true)
                    .key(Map.of(
                        "PK", AttributeValue.builder().s(partition).build(),
                        "SK", AttributeValue.builder().s(sortKey).build()
                    ))
                    .build());
                var item = response.item();
                check(item.containsKey("PK") && partition.equals(item.get("PK").s()) &&
                    item.containsKey("SK") && sortKey.equals(item.get("SK").s()) &&
                    item.containsKey("value") && "batch".equals(item.get("value").s()),
                    "DynamoDB did not return expected item PK=" + partition + " SK=" + sortKey);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        var endpoint = required("DAX_ENDPOINT");
        var region = System.getenv().getOrDefault("AWS_REGION", "eu-west-1");
        var table = System.getenv().getOrDefault("DAX_TABLE_NAME", "DaxParityTable");
        var tableCleanupEnabled = !"false".equalsIgnoreCase(System.getenv("DAX_FFI_TABLE_CLEANUP"));
        var config = new DaxClientConfig(
            List.of(endpoint), region, 60_000, 2, 2, 0, 70, 20_000, 10,
            4_000, 125, 30_000, 5_000, false, false,
            DaxIpDiscovery.DEFAULT, DaxLogLevel.OFF
        );
        try (var client = AwsDaxFfi.createDaxClient(config).join()) {
            try {
                faultBarrier(client);
            } catch (Exception | AssertionError error) {
                client.shutdown();
                throw error;
            }
            var partition = "FFI#JAVA#" + UUID.randomUUID();
            Map<String, DaxAttributeValue> key = Map.of(
                "PK", string(partition), "SK", string("ITEM#1")
            );
            var batchKeys = List.of("BATCH#1", "BATCH#2").stream().map(sortKey ->
                Map.of("PK", string(partition), "SK", string(sortKey))
            ).toList();
            var transactionKeys = List.of("TRANSACTION#1", "TRANSACTION#2").stream().map(sortKey ->
                Map.of("PK", string(partition), "SK", string(sortKey))
            ).toList();
            var postShutdownPaginator = client.queryPaginator(new DaxQueryRequest(
                table, null, "PK = :pk", null, null, Map.of(), Map.of(":pk", string(partition)),
                null, 1, null, null, null, null
            ));
            var inserted = false;
            try {
                var item = new HashMap<>(key);
                item.put("value", string("created"));
                client.putItem(new DaxPutItemRequest(
                    table, item, null, null, Map.of(), Map.of()
                )).join();
                inserted = true;

                var observed = false;
                for (var attempt = 0; attempt < 20; attempt++) {
                    var output = client.getItem(new DaxGetItemRequest(
                        table, key, null, null, Map.of()
                    )).join();
                    if (output.item() != null) {
                        observed = true;
                        break;
                    }
                    Thread.sleep(100);
                }
                check(observed, "GetItem did not observe the inserted item");

                var updated = client.updateItem(new DaxUpdateItemRequest(
                    table, key, "SET #value = :value", null,
                    Map.of("#value", "value"), Map.of(":value", string("updated")),
                    DaxReturnValue.ALL_OLD
                )).join();
                check(updated.attributes() != null, "UpdateItem did not return ALL_OLD");

                var query = client.query(new DaxQueryRequest(
                    table, null, "PK = :pk", null, null, Map.of(),
                    Map.of(":pk", string(partition)), null, 10, null, null, null, null
                )).join();
                check(!query.items().isEmpty(), "Query returned no items");

                var scan = client.scan(new DaxScanRequest(
                    table, null, "PK = :pk", null, Map.of(),
                    Map.of(":pk", string(partition)), null, null, null, null, null
                )).join();
                check(!scan.items().isEmpty(), "Scan returned no items");

                var batchWrite = client.batchWriteItem(new DaxBatchWriteItemRequest(
                    Map.of(table, batchKeys.stream().<DaxBatchWriteAction>map(batchKey ->
                        new DaxBatchWriteAction.Put(itemWithValue(batchKey, "batch"))
                    ).toList()), DaxReturnConsumedCapacity.TOTAL, null
                )).join();
                check(batchWrite.unprocessedItems().isEmpty(), "BatchWriteItem put returned unprocessed items");
                var batchGet = client.batchGetItem(new DaxBatchGetItemRequest(
                    Map.of(table, new DaxKeysAndAttributes(batchKeys, null, null, Map.of())),
                    DaxReturnConsumedCapacity.TOTAL
                )).join();
                var batchItems = batchGet.responses().get(table);
                check(batchItems != null && batchItems.size() == 2, "BatchGetItem did not return both items");
                check(batchGet.unprocessedKeys().isEmpty(), "BatchGetItem returned unprocessed keys");
                check(batchKeys.stream().allMatch(batchKey ->
                    batchItems.stream().anyMatch(itemValue -> matchesItem(itemValue, batchKey, "batch"))
                ), "BatchGetItem returned an unexpected item");
                verifyDynamoDbItems(region, table, partition);
                for (var stopOnDuplicateToken : List.of(false, true)) {
                    try (var paginator = client.batchGetItemPaginator(new DaxBatchGetItemRequest(
                        Map.of(table, new DaxKeysAndAttributes(batchKeys, null, null, Map.of())), null
                    ), stopOnDuplicateToken)) {
                        var page = paginator.nextPage().join();
                        check(page != null && page.responses().get(table).size() == batchKeys.size(),
                            "BatchGet paginator did not return its terminal page");
                        check(!paginator.hasMorePages(),
                            "BatchGet paginator retained a continuation without unprocessed keys");
                        check(paginator.nextPage().join() == null,
                            "BatchGet paginator returned a page after its terminal token");
                    }
                }
                try (var paginator = client.queryPaginator(new DaxQueryRequest(
                    table, null, "PK = :pk", null, null, Map.of(), Map.of(":pk", string(partition)),
                    null, 1, null, null, null, null
                ))) {
                    var pages = 0;
                    while (paginator.hasMorePages()) {
                        if (paginator.nextPage().join() == null) break;
                        pages++;
                    }
                    check(pages >= 2, "Query paginator did not traverse at least two pages");
                }
                try (var paginator = client.scanPaginator(new DaxScanRequest(
                    table, null, "PK = :pk", null, Map.of(), Map.of(":pk", string(partition)),
                    null, 1, null, null, null
                ))) {
                    var pages = 0;
                    while (paginator.hasMorePages() && pages < 100) {
                        if (paginator.nextPage().join() == null) break;
                        pages++;
                    }
                    check(pages >= 2, "Scan paginator did not traverse at least two pages");
                }
                var discovered = client.discoverEndpoints().join();
                check(!discovered.isEmpty(), "endpoint discovery returned no endpoints");
                check(client.activeEndpoints() != null && !client.activeEndpoints().isEmpty(),
                    "active endpoints were not committed by discovery");
                client.refreshEndpoints().join();
                check(!client.routeSnapshot().isEmpty(), "route snapshot was empty after endpoint refresh");
                if (tableCleanupEnabled) {
                    var batchDelete = client.batchWriteItem(new DaxBatchWriteItemRequest(
                        Map.of(table, batchKeys.stream().<DaxBatchWriteAction>map(DaxBatchWriteAction.Delete::new).toList()),
                        null, null
                    )).join();
                    check(batchDelete.unprocessedItems().isEmpty(), "BatchWriteItem delete returned unprocessed items");
                }

                client.transactWriteItems(new DaxTransactWriteItemsRequest(
                    transactionKeys.stream().<DaxTransactWriteAction>map(transactionKey ->
                        new DaxTransactWriteAction.Put(
                            table, itemWithValue(transactionKey, "transaction"), null, Map.of(), Map.of(), null
                        )
                    ).toList(), null, DaxReturnConsumedCapacity.TOTAL, null
                )).join();
                var transactionGet = client.transactGetItems(new DaxTransactGetItemsRequest(
                    transactionKeys.stream().map(transactionKey ->
                        new DaxTransactGetAction(table, transactionKey, null, Map.of())
                    ).toList(), DaxReturnConsumedCapacity.TOTAL
                )).join();
                check(transactionGet.responses().size() == 2, "TransactGetItems did not return both items");
                for (var i = 0; i < transactionKeys.size(); i++) {
                    check(matchesItem(transactionGet.responses().get(i).item(), transactionKeys.get(i), "transaction"),
                        "TransactGetItems returned an unexpected item");
                }

                try {
                    client.transactWriteItems(new DaxTransactWriteItemsRequest(
                        List.of(new DaxTransactWriteAction.ConditionCheck(
                            table, transactionKeys.getFirst(), "attribute_not_exists(PK)", Map.of(), Map.of(),
                            DaxReturnValuesOnConditionCheckFailure.ALL_OLD
                        )), null, null, null
                    )).join();
                    throw new AssertionError("failing transaction unexpectedly succeeded");
                } catch (CompletionException error) {
                    if (!(error.getCause() instanceof DaxFfiException.Failure failure)) {
                        throw error;
                    }
                    check(failure.details().category() == DaxErrorCategory.SERVICE,
                        "failing transaction was not a service error");
                    check(!failure.details().cancellationReasons().isEmpty(),
                        "failing transaction did not return cancellation reasons");
                }
                if (tableCleanupEnabled) {
                    client.transactWriteItems(new DaxTransactWriteItemsRequest(
                        transactionKeys.stream().<DaxTransactWriteAction>map(transactionKey ->
                            new DaxTransactWriteAction.Delete(table, transactionKey, null, Map.of(), Map.of(), null)
                        ).toList(), null, null, null
                    )).join();
                }
            } finally {
                try {
                    if (tableCleanupEnabled) {
                        var requireOld = inserted;
                        var cleanup = Stream.concat(
                            Stream.concat(batchKeys.stream(), transactionKeys.stream()), Stream.of(key)
                        ).map(cleanupKey -> client.deleteItem(new DaxDeleteItemRequest(
                            table, cleanupKey, cleanupKey == key ? DaxReturnValue.ALL_OLD : null,
                            null, Map.of(), Map.of()
                        )).thenAccept(deleted -> {
                            if (cleanupKey == key && requireOld) {
                                check(deleted.attributes() != null, "DeleteItem did not return ALL_OLD");
                            }
                        })).toArray(CompletableFuture<?>[]::new);
                        CompletableFuture.allOf(cleanup).join();
                    }
                } finally {
                    client.shutdown();
                    check(client.isClosed(), "client was not closed after shutdown");
                    try {
                        postShutdownPaginator.nextPage().join();
                        throw new AssertionError("post-shutdown paginator call unexpectedly succeeded");
                    } catch (CompletionException error) {
                        if (!(error.getCause() instanceof DaxFfiException.Failure)) throw error;
                    }
                    try {
                        client.getItem(new DaxGetItemRequest(table, key, null, null, Map.of())).join();
                        throw new AssertionError("post-shutdown operation unexpectedly succeeded");
                    } catch (CompletionException error) {
                        if (!(error.getCause() instanceof DaxFfiException.Failure)) throw error;
                    } finally {
                        postShutdownPaginator.close();
                    }
                }
            }
            if (!tableCleanupEnabled) System.out.println("Java FFI retained test items PK=" + partition);
        }
        System.out.println("Java FFI direct, batch, transaction, paginator, route, and lifecycle operations passed");
    }

    private static String required(String name) {
        var value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }
}
