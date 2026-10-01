import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import software.amazon.dax.*;

public final class Check {
  private static void liveOperations(DaxClient client) throws Exception {
    var table = System.getenv("DAX_TABLE_NAME");
    if (table == null || table.isEmpty()) table = "DaxParityTable";
    var id = UUID.randomUUID();
    Map<String, DaxAttributeValue> key = Map.of(
        "PK", new DaxAttributeValue.String("PACKAGE#JAVA#" + id),
        "SK", new DaxAttributeValue.String("ITEM#" + id));
    var item = new HashMap<>(key);
    item.put("value", new DaxAttributeValue.String("created"));
    var inserted = false;
    Exception failure = null;
    try {
      client.putItem(new DaxPutItemRequest(table, item, null, null, Map.of(), Map.of())).join();
      inserted = true;
      var observed = false;
      for (var attempt = 0; attempt < 20; attempt++) {
        var output = client.getItem(new DaxGetItemRequest(table, key, null, null, Map.of())).join();
        if (output.item() != null) {
          if (!item.equals(output.item())) throw new IllegalStateException("GetItem did not match the inserted item");
          observed = true;
          break;
        }
        if (attempt < 19) Thread.sleep(100);
      }
      if (!observed) throw new IllegalStateException("GetItem did not observe the inserted item after 20 attempts");
    } catch (Exception error) {
      failure = error;
      throw error;
    } finally {
      // A failed PutItem response can still mean a write.
      try {
        var output = client.deleteItem(new DaxDeleteItemRequest(
            table, key, DaxReturnValue.ALL_OLD, null, Map.of(), Map.of())).join();
        if (inserted && !item.equals(output.attributes()))
          throw new IllegalStateException("DeleteItem ALL_OLD did not match the inserted item");
      } catch (Exception error) {
        if (failure != null) failure.addSuppressed(error);
        else throw error;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (AwsDaxFfi.ffiContractVersion().isEmpty()) throw new AssertionError("empty FFI contract version");
    var live = "1".equals(System.getenv("DAX_FFI_LIVE"));
    var create = live || "1".equals(System.getenv("DAX_FFI_CREATE_CLIENT"));
    var endpoint = create ? System.getenv("DAX_ENDPOINT") : "127.0.0.1:8111";
    if (endpoint == null || endpoint.isEmpty()) throw new IllegalArgumentException("DAX_ENDPOINT is required");
    var config = new DaxClientConfig(List.of(endpoint), System.getenv("AWS_REGION"), 60_000, 2, 2, 0, 70,
        20_000, 10, 4_000, 125, 30_000, 5_000, false, false, DaxIpDiscovery.DEFAULT, DaxLogLevel.OFF);
    if (create) {
      try (var client = AwsDaxFfi.createDaxClient(config).join()) {
        try { if (live) liveOperations(client); }
        finally { client.shutdown(); }
      }
    }
    System.out.println(live ? "Java package live operations passed" : "java package native load ok");
  }
}
