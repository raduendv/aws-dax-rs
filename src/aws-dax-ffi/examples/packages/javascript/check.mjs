import { randomUUID } from "node:crypto";
import { setTimeout as delay } from "node:timers/promises";
import {
  ffiContractVersion, createDaxClient, DaxClientConfig, DaxIpDiscovery, DaxLogLevel,
  DaxAttributeValue, DaxPutItemRequest, DaxGetItemRequest, DaxDeleteItemRequest, DaxReturnValue,
} from "@raduendv/aws-dax-ffi";

if (!ffiContractVersion()) throw new Error("empty FFI contract version");
const live = process.env.DAX_FFI_LIVE === "1";
const create = live || process.env.DAX_FFI_CREATE_CLIENT === "1";
const endpoint = create ? process.env.DAX_ENDPOINT : "127.0.0.1:8111";
if (!endpoint) throw new Error("DAX_ENDPOINT is required");
const config = DaxClientConfig.create({
  endpoints: [endpoint], region: process.env.AWS_REGION,
  requestTimeoutMillis: 60_000n, writeRetries: 2, readRetries: 2, retryDelayMillis: 0n,
  throttleBaseDelayMillis: 70n, throttleMaxBackoffMillis: 20_000n,
  maxPendingConnectionsPerHost: 10, clusterUpdateIntervalMillis: 4_000n,
  clusterUpdateThresholdMillis: 125n, idleConnectionReapDelayMillis: 30_000n,
  clientHealthCheckIntervalMillis: 5_000n, skipHostnameVerification: false,
  routeManagerEnabled: false, ipDiscovery: DaxIpDiscovery.Default, logLevel: DaxLogLevel.Off,
});

async function liveOperations(client) {
  const tableName = process.env.DAX_TABLE_NAME || "DaxParityTable";
  const id = randomUUID();
  const stringValue = (value) => new DaxAttributeValue.String({ string: value });
  const key = new Map([
    ["PK", stringValue(`PACKAGE#NODE#${id}`)], ["SK", stringValue(`ITEM#${id}`)],
  ]);
  const item = new Map([...key, ["value", stringValue("created")]]);
  const matches = (actual) => actual !== undefined && actual.size === item.size &&
    [...item].every(([name, expected]) => {
      const value = actual.get(name);
      return value !== undefined && DaxAttributeValue.String.instanceOf(value) &&
        value.inner.string === expected.inner.string;
    });
  let inserted = false;
  const errors = [];
  try {
    await client.putItem(DaxPutItemRequest.create({
      tableName, item, expressionAttributeNames: new Map(), expressionAttributeValues: new Map(),
    }));
    inserted = true;
    let observed = false;
    for (let attempt = 0; attempt < 20; attempt++) {
      const output = await client.getItem(DaxGetItemRequest.create({
        tableName, key, expressionAttributeNames: new Map(),
      }));
      if (output.item !== undefined) {
        if (!matches(output.item)) throw new Error("GetItem did not match the inserted item");
        observed = true;
        break;
      }
      if (attempt < 19) await delay(100);
    }
    if (!observed) throw new Error("GetItem did not observe the inserted item after 20 attempts");
  } catch (error) {
    errors.push(error);
  } finally {
    // A failed PutItem response can still mean a write.
    try {
      const output = await client.deleteItem(DaxDeleteItemRequest.create({
        tableName, key, returnValues: DaxReturnValue.AllOld,
        expressionAttributeNames: new Map(), expressionAttributeValues: new Map(),
      }));
      if (inserted && !matches(output.attributes))
        throw new Error("DeleteItem ALL_OLD did not match the inserted item");
    } catch (error) {
      errors.push(error);
    }
  }
  if (errors.length) throw new AggregateError(errors, "Live package operations or cleanup failed");
}

if (create) {
  const client = await createDaxClient(config);
  try {
    if (live) await liveOperations(client);
  } finally {
    try { client.shutdown(); } finally { client.uniffiDestroy(); }
  }
}
console.log(live ? "Node/N-API package live operations passed" : "javascript package native load ok");
