using Aws.Dax;
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System.Diagnostics;

static DaxAttributeValue StringValue(string value) => new DaxAttributeValue.String(value);
static void Check(bool condition, string message)
{
    if (!condition) throw new InvalidOperationException(message);
}
static Dictionary<string, DaxAttributeValue> ItemWithValue(
    Dictionary<string, DaxAttributeValue> key, string value) =>
    new(key) { ["value"] = StringValue(value) };
static bool MatchesItem(Dictionary<string, DaxAttributeValue>? item,
    Dictionary<string, DaxAttributeValue> key, string value) =>
    item is not null && ItemWithValue(key, value).All(pair =>
        item.TryGetValue(pair.Key, out var actual) && actual.Equals(pair.Value));
static async Task VerifyDynamoDbItems(string region, string table, string partition)
{
    using var dynamoDb = new AmazonDynamoDBClient(RegionEndpoint.GetBySystemName(region));
    foreach (var sortKey in new[] { "BATCH#1", "BATCH#2" })
    {
        var response = await dynamoDb.GetItemAsync(new GetItemRequest
        {
            TableName = table,
            ConsistentRead = true,
            Key = new()
            {
                ["PK"] = new AttributeValue { S = partition },
                ["SK"] = new AttributeValue { S = sortKey },
            },
        });
        Check(response.Item.TryGetValue("PK", out var pk) && pk.S == partition &&
            response.Item.TryGetValue("SK", out var sk) && sk.S == sortKey &&
            response.Item.TryGetValue("value", out var value) && value.S == "batch",
            $"DynamoDB did not return expected item PK={partition} SK={sortKey}");
    }
}

static async Task FaultBarrier(DaxClient client)
{
    var ready = Environment.GetEnvironmentVariable("DAX_FAULT_READY_FILE");
    var release = Environment.GetEnvironmentVariable("DAX_FAULT_RELEASE_FILE");
    if (string.IsNullOrEmpty(ready) || string.IsNullOrEmpty(release)) return;
    Check((await client.DiscoverEndpoints()).Length > 0, "fault prewarm discovered no endpoints");
    await client.RefreshEndpoints();
    Check(client.RouteSnapshot().Length > 0, "fault prewarm has no routes");
    using (new FileStream(ready, FileMode.CreateNew, FileAccess.Write)) { }
    var timer = Stopwatch.StartNew();
    while (timer.Elapsed < TimeSpan.FromSeconds(120))
    {
        try
        {
            using var file = File.OpenRead(release);
            return;
        }
        catch (FileNotFoundException) { }
        await Task.Delay(100);
    }
    throw new TimeoutException("timed out waiting for DAX_FAULT_RELEASE_FILE after 120 seconds");
}

var endpoint = Environment.GetEnvironmentVariable("DAX_ENDPOINT")
    ?? throw new InvalidOperationException("DAX_ENDPOINT is required");
var region = Environment.GetEnvironmentVariable("AWS_REGION") ?? "eu-west-1";
var table = Environment.GetEnvironmentVariable("DAX_TABLE_NAME") ?? "DaxParityTable";
var tableCleanupEnabled = !string.Equals(
    Environment.GetEnvironmentVariable("DAX_FFI_TABLE_CLEANUP"), "false", StringComparison.OrdinalIgnoreCase);
var config = new DaxClientConfig(
    Endpoints: [endpoint], Region: region, RequestTimeoutMillis: 60_000,
    WriteRetries: 2, ReadRetries: 2, RetryDelayMillis: 0,
    ThrottleBaseDelayMillis: 70, ThrottleMaxBackoffMillis: 20_000,
    MaxPendingConnectionsPerHost: 10, ClusterUpdateIntervalMillis: 4_000,
    ClusterUpdateThresholdMillis: 125, IdleConnectionReapDelayMillis: 30_000,
    ClientHealthCheckIntervalMillis: 5_000, SkipHostnameVerification: false,
    RouteManagerEnabled: false, IpDiscovery: DaxIpDiscovery.Default,
    LogLevel: DaxLogLevel.Off
);
using var client = await AwsDax.CreateDaxClient(config);
try { await FaultBarrier(client); }
catch
{
    client.Shutdown();
    throw;
}
var partition = $"FFI#DOTNET#{Guid.NewGuid():N}";
var key = new Dictionary<string, DaxAttributeValue>
{
    ["PK"] = StringValue(partition),
    ["SK"] = StringValue("ITEM#1"),
};
var batchKeys = new[] { "BATCH#1", "BATCH#2" }.Select(sortKey =>
    new Dictionary<string, DaxAttributeValue> { ["PK"] = StringValue(partition), ["SK"] = StringValue(sortKey) }
).ToArray();
var transactionKeys = new[] { "TRANSACTION#1", "TRANSACTION#2" }.Select(sortKey =>
    new Dictionary<string, DaxAttributeValue> { ["PK"] = StringValue(partition), ["SK"] = StringValue(sortKey) }
).ToArray();
using var postShutdownPaginator = client.QueryPaginator(new DaxQueryRequest(
    table, null, "PK = :pk", null, null, [],
    new() { [":pk"] = StringValue(partition) }, null, 1, null, null, null, null
));
var inserted = false;
try
{
    var item = new Dictionary<string, DaxAttributeValue>(key)
    {
        ["value"] = StringValue("created"),
    };
    await client.PutItem(new DaxPutItemRequest(
        TableName: table, Item: item, ReturnValues: null, ConditionExpression: null,
        ExpressionAttributeNames: [], ExpressionAttributeValues: []
    ));
    inserted = true;

    var observed = false;
    for (var attempt = 0; attempt < 20; attempt++)
    {
        var output = await client.GetItem(new DaxGetItemRequest(
            TableName: table, Key: key, ConsistentRead: null,
            ProjectionExpression: null, ExpressionAttributeNames: []
        ));
        if (output.Item is not null)
        {
            observed = true;
            break;
        }
        await Task.Delay(100);
    }
    Check(observed, "GetItem did not observe the inserted item");

    var updated = await client.UpdateItem(new DaxUpdateItemRequest(
        TableName: table, Key: key, UpdateExpression: "SET #value = :value",
        ConditionExpression: null,
        ExpressionAttributeNames: new() { ["#value"] = "value" },
        ExpressionAttributeValues: new() { [":value"] = StringValue("updated") },
        ReturnValues: DaxReturnValue.AllOld
    ));
    Check(updated.Attributes is not null, "UpdateItem did not return ALL_OLD");

    var query = await client.Query(new DaxQueryRequest(
        TableName: table, IndexName: null, KeyConditionExpression: "PK = :pk",
        FilterExpression: null, ProjectionExpression: null,
        ExpressionAttributeNames: [],
        ExpressionAttributeValues: new() { [":pk"] = StringValue(partition) },
        ConsistentRead: null, Limit: 10, ScanIndexForward: null, Select: null,
        ReturnConsumedCapacity: null, ExclusiveStartKey: null
    ));
    Check(query.Items.Length > 0, "Query returned no items");

    var scan = await client.Scan(new DaxScanRequest(
        TableName: table, IndexName: null, FilterExpression: "PK = :pk",
        ProjectionExpression: null, ExpressionAttributeNames: [],
        ExpressionAttributeValues: new() { [":pk"] = StringValue(partition) },
        ConsistentRead: null, Limit: null, Select: null,
        ReturnConsumedCapacity: null, ExclusiveStartKey: null
    ));
    Check(scan.Items.Length > 0, "Scan returned no items");

    var batchWrite = await client.BatchWriteItem(new DaxBatchWriteItemRequest(
        RequestItems: new() { [table] = batchKeys.Select(batchKey =>
            new DaxBatchWriteAction.Put(ItemWithValue(batchKey, "batch"))).ToArray() },
        ReturnConsumedCapacity: DaxReturnConsumedCapacity.Total, ReturnItemCollectionMetrics: null
    ));
    Check(batchWrite.UnprocessedItems.Count == 0, "BatchWriteItem put returned unprocessed items");
    var batchGet = await client.BatchGetItem(new DaxBatchGetItemRequest(
        RequestItems: new() { [table] = new DaxKeysAndAttributes(batchKeys, null, null, []) },
        ReturnConsumedCapacity: DaxReturnConsumedCapacity.Total
    ));
    Check(batchGet.Responses.TryGetValue(table, out var batchItems) && batchItems.Length == 2,
        "BatchGetItem did not return both items");
    Check(batchGet.UnprocessedKeys.Count == 0, "BatchGetItem returned unprocessed keys");
    Check(batchKeys.All(batchKey => batchGet.Responses[table].Any(item => MatchesItem(item, batchKey, "batch"))),
        "BatchGetItem returned an unexpected item");
    await VerifyDynamoDbItems(region, table, partition);
    foreach (var stopOnDuplicateToken in new[] { false, true })
    {
        using var paginator = client.BatchGetItemPaginator(new DaxBatchGetItemRequest(
            RequestItems: new() { [table] = new DaxKeysAndAttributes(batchKeys, null, null, []) },
            ReturnConsumedCapacity: null
        ), stopOnDuplicateToken);
        var page = await paginator.NextPage();
        Check(page is not null && page.Responses[table].Length == batchKeys.Length,
            "BatchGet paginator did not return its terminal page");
        Check(!paginator.HasMorePages(), "BatchGet paginator retained a continuation without unprocessed keys");
        Check(await paginator.NextPage() is null, "BatchGet paginator returned a page after its terminal token");
    }
    using (var paginator = client.QueryPaginator(new DaxQueryRequest(
        table, null, "PK = :pk", null, null, [],
        new() { [":pk"] = StringValue(partition) }, null, 1, null, null, null, null
    )))
    {
        var pages = 0;
        while (paginator.HasMorePages())
        {
            if (await paginator.NextPage() is null) break;
            pages++;
        }
        Check(pages >= 2, "Query paginator did not traverse at least two pages");
    }
    using (var paginator = client.ScanPaginator(new DaxScanRequest(
        table, null, "PK = :pk", null, [], new() { [":pk"] = StringValue(partition) },
        null, 1, null, null, null
    )))
    {
        var pages = 0;
        while (paginator.HasMorePages() && pages < 100)
        {
            if (await paginator.NextPage() is null) break;
            pages++;
        }
        Check(pages >= 2, "Scan paginator did not traverse at least two pages");
    }
    var discovered = await client.DiscoverEndpoints();
    Check(discovered.Length > 0, "endpoint discovery returned no endpoints");
    Check(client.ActiveEndpoints() is { Length: > 0 }, "active endpoints were not committed by discovery");
    await client.RefreshEndpoints();
    Check(client.RouteSnapshot().Length > 0, "route snapshot was empty after endpoint refresh");
    if (tableCleanupEnabled)
    {
        var batchDelete = await client.BatchWriteItem(new DaxBatchWriteItemRequest(
            RequestItems: new() { [table] = batchKeys.Select(batchKey => new DaxBatchWriteAction.Delete(batchKey)).ToArray() },
            ReturnConsumedCapacity: null, ReturnItemCollectionMetrics: null
        ));
        Check(batchDelete.UnprocessedItems.Count == 0, "BatchWriteItem delete returned unprocessed items");
    }

    await client.TransactWriteItems(new DaxTransactWriteItemsRequest(
        TransactItems: transactionKeys.Select(transactionKey => new DaxTransactWriteAction.Put(
            table, ItemWithValue(transactionKey, "transaction"), null, [], [], null
        )).ToArray(),
        ClientRequestToken: null, ReturnConsumedCapacity: DaxReturnConsumedCapacity.Total,
        ReturnItemCollectionMetrics: null
    ));
    var transactionGet = await client.TransactGetItems(new DaxTransactGetItemsRequest(
        TransactItems: transactionKeys.Select(transactionKey => new DaxTransactGetAction(
            table, transactionKey, null, []
        )).ToArray(),
        ReturnConsumedCapacity: DaxReturnConsumedCapacity.Total
    ));
    Check(transactionGet.Responses.Length == 2, "TransactGetItems did not return both items");
    for (var i = 0; i < transactionKeys.Length; i++)
        Check(MatchesItem(transactionGet.Responses[i].Item, transactionKeys[i], "transaction"),
            "TransactGetItems returned an unexpected item");

    try
    {
        await client.TransactWriteItems(new DaxTransactWriteItemsRequest(
            TransactItems: [new DaxTransactWriteAction.ConditionCheck(
                table, transactionKeys[0], "attribute_not_exists(PK)", [], [],
                DaxReturnValuesOnConditionCheckFailure.AllOld
            )],
            ClientRequestToken: null, ReturnConsumedCapacity: null, ReturnItemCollectionMetrics: null
        ));
        throw new InvalidOperationException("failing transaction unexpectedly succeeded");
    }
    catch (DaxFfiException.Failure error)
    {
        Check(error.details.Category == DaxErrorCategory.Service, "failing transaction was not a service error");
        Check(error.details.CancellationReasons.Length > 0, "failing transaction did not return cancellation reasons");
    }
    if (tableCleanupEnabled)
    {
        await client.TransactWriteItems(new DaxTransactWriteItemsRequest(
            TransactItems: transactionKeys.Select(transactionKey => new DaxTransactWriteAction.Delete(
                table, transactionKey, null, [], [], null
            )).ToArray(),
            ClientRequestToken: null, ReturnConsumedCapacity: null, ReturnItemCollectionMetrics: null
        ));
    }
}
finally
{
    try
    {
        if (tableCleanupEnabled)
        {
            await Task.WhenAll(batchKeys.Concat(transactionKeys).Append(key).Select(async cleanupKey =>
            {
                var directItem = ReferenceEquals(cleanupKey, key);
                var deleted = await client.DeleteItem(new DaxDeleteItemRequest(
                    TableName: table, Key: cleanupKey, ReturnValues: directItem ? DaxReturnValue.AllOld : null,
                    ConditionExpression: null, ExpressionAttributeNames: [],
                    ExpressionAttributeValues: []
                ));
                if (directItem && inserted)
                    Check(deleted.Attributes is not null, "DeleteItem did not return ALL_OLD");
            }));
        }
    }
    finally
    {
        client.Shutdown();
    }
}

Check(client.IsClosed(), "client was not closed after shutdown");
try { await postShutdownPaginator.NextPage(); throw new InvalidOperationException("post-shutdown paginator call unexpectedly succeeded"); }
catch (DaxFfiException.Failure) { }
try
{
    await client.GetItem(new DaxGetItemRequest(table, key, null, null, []));
    throw new InvalidOperationException("post-shutdown operation unexpectedly succeeded");
}
catch (DaxFfiException.Failure) { }
if (!tableCleanupEnabled) Console.WriteLine($".NET FFI retained test items PK={partition}");
Console.WriteLine(".NET FFI direct, batch, transaction, paginator, route, and lifecycle operations passed");
