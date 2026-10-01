using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Aws.Dax;

static bool Matches(Dictionary<string, DaxAttributeValue>? actual,
    Dictionary<string, DaxAttributeValue> expected) =>
    actual is not null && actual.Count == expected.Count &&
    expected.All(pair => actual.TryGetValue(pair.Key, out var value) && value.Equals(pair.Value));

static async Task LiveOperations(DaxClient client)
{
    var table = Environment.GetEnvironmentVariable("DAX_TABLE_NAME");
    if (string.IsNullOrEmpty(table)) table = "DaxParityTable";
    var id = Guid.NewGuid().ToString("N");
    var key = new Dictionary<string, DaxAttributeValue> {
        ["PK"] = new DaxAttributeValue.String($"PACKAGE#DOTNET#{id}"),
        ["SK"] = new DaxAttributeValue.String($"ITEM#{id}"),
    };
    var item = new Dictionary<string, DaxAttributeValue>(key) {
        ["value"] = new DaxAttributeValue.String("created"),
    };
    var inserted = false;
    var errors = new List<Exception>();
    try
    {
        await client.PutItem(new DaxPutItemRequest(table, item, null, null, [], []));
        inserted = true;
        var observed = false;
        for (var attempt = 0; attempt < 20; attempt++)
        {
            var output = await client.GetItem(new DaxGetItemRequest(table, key, null, null, []));
            if (output.Item is not null)
            {
                if (!Matches(output.Item, item)) throw new Exception("GetItem did not match the inserted item");
                observed = true;
                break;
            }
            if (attempt < 19) await Task.Delay(100);
        }
        if (!observed) throw new Exception("GetItem did not observe the inserted item after 20 attempts");
    }
    catch (Exception error) { errors.Add(error); }
    finally
    {
        // A failed PutItem response can still mean a write.
        try
        {
            var output = await client.DeleteItem(new DaxDeleteItemRequest(
                table, key, DaxReturnValue.AllOld, null, [], []));
            if (inserted && !Matches(output.Attributes, item))
                throw new Exception("DeleteItem ALL_OLD did not match the inserted item");
        }
        catch (Exception error) { errors.Add(error); }
    }
    if (errors.Count > 0) throw new AggregateException(errors);
}

if (AwsDax.FfiContractVersion().Length == 0) throw new Exception("empty FFI contract version");
var live = Environment.GetEnvironmentVariable("DAX_FFI_LIVE") == "1";
var config = new DaxClientConfig(["127.0.0.1:8111"], Environment.GetEnvironmentVariable("AWS_REGION"),
    60_000, 2, 2, 0, 70, 20_000, 10, 4_000, 125, 30_000, 5_000, false, false, DaxIpDiscovery.Default, DaxLogLevel.Off);
if (live || Environment.GetEnvironmentVariable("DAX_FFI_CREATE_CLIENT") == "1") {
    var endpoint = Environment.GetEnvironmentVariable("DAX_ENDPOINT");
    if (string.IsNullOrEmpty(endpoint)) throw new Exception("DAX_ENDPOINT is required");
    config = config with { Endpoints = [endpoint] };
    using var client = await AwsDax.CreateDaxClient(config);
    try { if (live) await LiveOperations(client); }
    finally { client.Shutdown(); }
}
Console.WriteLine(live ? ".NET package live operations passed" : "dotnet package native load ok");
