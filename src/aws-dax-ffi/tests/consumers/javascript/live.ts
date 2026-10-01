import { randomUUID } from "node:crypto";
import { open, stat } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { DynamoDBClient, GetItemCommand } from "@aws-sdk/client-dynamodb";
import {
  createDaxClient,
  DaxAttributeValue,
  DaxBatchGetItemRequest,
  DaxBatchWriteAction,
  DaxBatchWriteItemRequest,
  DaxClientConfig,
  DaxDeleteItemRequest,
  DaxErrorCategory,
  DaxFfiError,
  DaxGetItemRequest,
  DaxIpDiscovery,
  DaxKeysAndAttributes,
  DaxLogLevel,
  DaxPutItemRequest,
  DaxQueryRequest,
  DaxReturnConsumedCapacity,
  DaxReturnValue,
  DaxReturnValuesOnConditionCheckFailure,
  DaxScanRequest,
  DaxTransactGetAction,
  DaxTransactGetItemsRequest,
  DaxTransactWriteAction,
  DaxTransactWriteItemsRequest,
  DaxUpdateItemRequest,
} from "./generated/index.ts";

const stringValue = (value: string) =>
  new DaxAttributeValue.String({ string: value });
const itemWithValue = (key: Map<string, DaxAttributeValue>, value: string) =>
  new Map([...key, ["value", stringValue(value)]]);
const matchesItem = (
  item: Map<string, DaxAttributeValue> | undefined,
  key: Map<string, DaxAttributeValue>,
  value: string,
) => item !== undefined && [...itemWithValue(key, value)].every(([name, expected]) => {
  const actual = item.get(name);
  return actual !== undefined && DaxAttributeValue.String.instanceOf(actual)
    && DaxAttributeValue.String.instanceOf(expected) && actual.inner.string === expected.inner.string;
});
const verifyDynamoDbItems = async (region: string, table: string, partition: string) => {
  const dynamoDb = new DynamoDBClient({ region });
  try {
    for (const sortKey of ["BATCH#1", "BATCH#2"]) {
      const response = await dynamoDb.send(new GetItemCommand({
        TableName: table,
        ConsistentRead: true,
        Key: { PK: { S: partition }, SK: { S: sortKey } },
      }));
      if (response.Item?.PK?.S !== partition || response.Item.SK?.S !== sortKey || response.Item.value?.S !== "batch") {
        throw new Error(`DynamoDB did not return expected item PK=${partition} SK=${sortKey}`);
      }
    }
  } finally {
    dynamoDb.destroy();
  }
};

const endpoint = process.env.DAX_ENDPOINT;
if (!endpoint) throw new Error("DAX_ENDPOINT is required");
const region = process.env.AWS_REGION ?? "eu-west-1";
const table = process.env.DAX_TABLE_NAME ?? "DaxParityTable";
const tableCleanupEnabled = process.env.DAX_FFI_TABLE_CLEANUP?.toLowerCase() !== "false";
const config = DaxClientConfig.create({
  endpoints: [endpoint],
  region,
  requestTimeoutMillis: 60_000n,
  writeRetries: 2,
  readRetries: 2,
  retryDelayMillis: 0n,
  throttleBaseDelayMillis: 70n,
  throttleMaxBackoffMillis: 20_000n,
  maxPendingConnectionsPerHost: 10,
  clusterUpdateIntervalMillis: 4_000n,
  clusterUpdateThresholdMillis: 125n,
  idleConnectionReapDelayMillis: 30_000n,
  clientHealthCheckIntervalMillis: 5_000n,
  skipHostnameVerification: false,
  routeManagerEnabled: false,
  ipDiscovery: DaxIpDiscovery.Default,
  logLevel: DaxLogLevel.Off,
});
const client = await createDaxClient(config);
async function faultBarrier() {
  const ready = process.env.DAX_FAULT_READY_FILE;
  const release = process.env.DAX_FAULT_RELEASE_FILE;
  if (!ready || !release) return;
  if ((await client.discoverEndpoints()).length === 0) {
    throw new Error("fault prewarm discovered no endpoints");
  }
  await client.refreshEndpoints();
  if (client.routeSnapshot().length === 0) throw new Error("fault prewarm has no routes");
  const file = await open(ready, "wx", 0o600);
  await file.close();
  const start = performance.now();
  while (performance.now() - start < 120_000) {
    try {
      if (!(await stat(release)).isFile()) throw new Error("fault release path is not a regular file");
      return;
    } catch (error) {
      if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
    }
    await delay(100);
  }
  throw new Error("timed out waiting for DAX_FAULT_RELEASE_FILE after 120 seconds");
}
try {
  await faultBarrier();
} catch (error) {
  client.shutdown();
  throw error;
}
const partition = `FFI#JAVASCRIPT#${randomUUID()}`;
const key = new Map([
  ["PK", stringValue(partition)],
  ["SK", stringValue("ITEM#1")],
]);
const batchKeys = ["BATCH#1", "BATCH#2"].map((sortKey) => new Map([
  ["PK", stringValue(partition)], ["SK", stringValue(sortKey)],
]));
const transactionKeys = ["TRANSACTION#1", "TRANSACTION#2"].map((sortKey) => new Map([
  ["PK", stringValue(partition)], ["SK", stringValue(sortKey)],
]));
const postShutdownPaginator = client.queryPaginator(DaxQueryRequest.create({
  tableName: table, keyConditionExpression: "PK = :pk",
  expressionAttributeNames: new Map(),
  expressionAttributeValues: new Map([[":pk", stringValue(partition)]]), limit: 1,
}));
let inserted = false;
try {
  const item = new Map(key);
  item.set("value", stringValue("created"));
  await client.putItem(DaxPutItemRequest.create({
    tableName: table,
    item,
    expressionAttributeNames: new Map(),
    expressionAttributeValues: new Map(),
  }));
  inserted = true;

  let observed = false;
  for (let attempt = 0; attempt < 20; attempt++) {
    const output = await client.getItem(DaxGetItemRequest.create({
      tableName: table,
      key,
      expressionAttributeNames: new Map(),
    }));
    if (output.item !== undefined) {
      observed = true;
      break;
    }
    await new Promise((resolve) => setTimeout(resolve, 100));
  }
  if (!observed) throw new Error("GetItem did not observe the inserted item");

  const updated = await client.updateItem(DaxUpdateItemRequest.create({
    tableName: table,
    key,
    updateExpression: "SET #value = :value",
    expressionAttributeNames: new Map([["#value", "value"]]),
    expressionAttributeValues: new Map([[":value", stringValue("updated")]]),
    returnValues: DaxReturnValue.AllOld,
  }));
  if (updated.attributes === undefined) {
    throw new Error("UpdateItem did not return ALL_OLD");
  }

  const query = await client.query(DaxQueryRequest.create({
    tableName: table,
    keyConditionExpression: "PK = :pk",
    expressionAttributeNames: new Map(),
    expressionAttributeValues: new Map([[":pk", stringValue(partition)]]),
    limit: 10,
  }));
  if (query.items.length === 0) throw new Error("Query returned no items");

  const scan = await client.scan(DaxScanRequest.create({
    tableName: table,
    filterExpression: "PK = :pk",
    expressionAttributeNames: new Map(),
    expressionAttributeValues: new Map([[":pk", stringValue(partition)]]),
  }));
  if (scan.items.length === 0) throw new Error("Scan returned no items");

  const batchWrite = await client.batchWriteItem(DaxBatchWriteItemRequest.create({
    requestItems: new Map([[table, batchKeys.map((batchKey) =>
      new DaxBatchWriteAction.Put({ item: itemWithValue(batchKey, "batch") }))]]),
    returnConsumedCapacity: DaxReturnConsumedCapacity.Total,
  }));
  if (batchWrite.unprocessedItems.size !== 0) throw new Error("BatchWriteItem put returned unprocessed items");
  const batchGet = await client.batchGetItem(DaxBatchGetItemRequest.create({
    requestItems: new Map([[table, DaxKeysAndAttributes.create({
      keys: batchKeys, expressionAttributeNames: new Map(),
    })]]),
    returnConsumedCapacity: DaxReturnConsumedCapacity.Total,
  }));
  const batchItems = batchGet.responses.get(table);
  if (batchItems === undefined || batchItems.length !== 2) throw new Error("BatchGetItem did not return both items");
  if (batchGet.unprocessedKeys.size !== 0) throw new Error("BatchGetItem returned unprocessed keys");
  if (!batchKeys.every((batchKey) => batchItems.some((item) => matchesItem(item, batchKey, "batch")))) {
    throw new Error("BatchGetItem returned an unexpected item");
  }
  await verifyDynamoDbItems(region, table, partition);
  for (const stopOnDuplicateToken of [false, true]) {
    const paginator = client.batchGetItemPaginator(DaxBatchGetItemRequest.create({
      requestItems: new Map([[table, DaxKeysAndAttributes.create({
        keys: batchKeys, expressionAttributeNames: new Map(),
      })]]),
    }), stopOnDuplicateToken);
    const page = await paginator.nextPage();
    if (page === undefined || page.responses.get(table)?.length !== batchKeys.length) {
      throw new Error("BatchGet paginator did not return its terminal page");
    }
    if (paginator.hasMorePages()) {
      throw new Error("BatchGet paginator retained a continuation without unprocessed keys");
    }
    if (await paginator.nextPage() !== undefined) {
      throw new Error("BatchGet paginator returned a page after its terminal token");
    }
  }
  const queryPaginator = client.queryPaginator(DaxQueryRequest.create({
    tableName: table, keyConditionExpression: "PK = :pk",
    expressionAttributeNames: new Map(),
    expressionAttributeValues: new Map([[":pk", stringValue(partition)]]), limit: 1,
  }));
  let pages = 0;
  while (queryPaginator.hasMorePages()) {
    if (await queryPaginator.nextPage() === undefined) break;
    pages++;
  }
  if (pages < 2) throw new Error("Query paginator did not traverse at least two pages");
  const scanPaginator = client.scanPaginator(DaxScanRequest.create({
    tableName: table, filterExpression: "PK = :pk",
    expressionAttributeNames: new Map(),
    expressionAttributeValues: new Map([[":pk", stringValue(partition)]]), limit: 1,
  }));
  pages = 0;
  while (scanPaginator.hasMorePages() && pages < 100) {
    if (await scanPaginator.nextPage() === undefined) break;
    pages++;
  }
  if (pages < 2) throw new Error("Scan paginator did not traverse at least two pages");
  const discovered = await client.discoverEndpoints();
  if (discovered.length === 0 || client.activeEndpoints() === undefined) {
    throw new Error("endpoint discovery did not commit active endpoints");
  }
  await client.refreshEndpoints();
  if (client.routeSnapshot().length === 0) throw new Error("route snapshot was empty after endpoint refresh");
  if (tableCleanupEnabled) {
    const batchDelete = await client.batchWriteItem(DaxBatchWriteItemRequest.create({
      requestItems: new Map([[table, batchKeys.map((batchKey) =>
        new DaxBatchWriteAction.Delete({ key: batchKey }))]]),
    }));
    if (batchDelete.unprocessedItems.size !== 0) throw new Error("BatchWriteItem delete returned unprocessed items");
  }

  await client.transactWriteItems(DaxTransactWriteItemsRequest.create({
    transactItems: transactionKeys.map((transactionKey) => new DaxTransactWriteAction.Put({
      tableName: table, item: itemWithValue(transactionKey, "transaction"),
      expressionAttributeNames: new Map(), expressionAttributeValues: new Map(),
    })),
    returnConsumedCapacity: DaxReturnConsumedCapacity.Total,
  }));
  const transactionGet = await client.transactGetItems(DaxTransactGetItemsRequest.create({
    transactItems: transactionKeys.map((transactionKey) => DaxTransactGetAction.create({
      tableName: table, key: transactionKey, expressionAttributeNames: new Map(),
    })),
    returnConsumedCapacity: DaxReturnConsumedCapacity.Total,
  }));
  if (transactionGet.responses.length !== 2) throw new Error("TransactGetItems did not return both items");
  if (!transactionKeys.every((transactionKey, i) =>
    matchesItem(transactionGet.responses[i].item, transactionKey, "transaction"))) {
    throw new Error("TransactGetItems returned an unexpected item");
  }
  try {
    await client.transactWriteItems(DaxTransactWriteItemsRequest.create({
      transactItems: [new DaxTransactWriteAction.ConditionCheck({
        tableName: table, key: transactionKeys[0], conditionExpression: "attribute_not_exists(PK)",
        expressionAttributeNames: new Map(), expressionAttributeValues: new Map(),
        returnValuesOnConditionCheckFailure: DaxReturnValuesOnConditionCheckFailure.AllOld,
      })],
    }));
    throw new Error("failing transaction unexpectedly succeeded");
  } catch (error) {
    if (!(error instanceof DaxFfiError.Failure)) throw error;
    if (error.inner.details.category !== DaxErrorCategory.Service) {
      throw new Error("failing transaction was not a service error", { cause: error });
    }
    if (error.inner.details.cancellationReasons.length === 0) {
      throw new Error("failing transaction did not return cancellation reasons", { cause: error });
    }
  }
  if (tableCleanupEnabled) {
    await client.transactWriteItems(DaxTransactWriteItemsRequest.create({
      transactItems: transactionKeys.map((transactionKey) => new DaxTransactWriteAction.Delete({
        tableName: table, key: transactionKey,
        expressionAttributeNames: new Map(), expressionAttributeValues: new Map(),
      })),
    }));
  }
} finally {
  try {
    if (tableCleanupEnabled) {
      const cleanup = await Promise.allSettled([...batchKeys, ...transactionKeys, key].map(async (cleanupKey) => {
        const deleted = await client.deleteItem(DaxDeleteItemRequest.create({
          tableName: table,
          key: cleanupKey,
          returnValues: cleanupKey === key ? DaxReturnValue.AllOld : undefined,
          expressionAttributeNames: new Map(),
          expressionAttributeValues: new Map(),
        }));
        if (cleanupKey === key && inserted && deleted.attributes === undefined) {
          throw new Error("DeleteItem did not return ALL_OLD");
        }
      }));
      const failures = cleanup.filter((result) => result.status === "rejected");
      if (failures.length !== 0) {
        throw new AggregateError(failures.map((result) => result.reason), "DeleteItem cleanup failed");
      }
    }
  } finally {
    client.shutdown();
  }
}
  if (!tableCleanupEnabled) console.log(`JavaScript FFI retained test items PK=${partition}`);

if (!client.isClosed()) throw new Error("client was not closed after shutdown");
try {
  await postShutdownPaginator.nextPage();
  throw new Error("post-shutdown paginator call unexpectedly succeeded");
} catch (error) {
  if (!(error instanceof DaxFfiError.Failure)) throw error;
}
try {
  await client.getItem(DaxGetItemRequest.create({
    tableName: table, key, expressionAttributeNames: new Map(),
  }));
  throw new Error("post-shutdown operation unexpectedly succeeded");
} catch (error) {
  if (!(error instanceof DaxFfiError.Failure)) throw error;
}
console.log("JavaScript FFI direct, batch, transaction, paginator, route, and lifecycle operations passed");
