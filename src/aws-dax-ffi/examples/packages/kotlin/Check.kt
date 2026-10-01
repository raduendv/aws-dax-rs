import software.amazon.dax.*
import java.util.UUID
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext

private suspend fun liveOperations(client: DaxClient) {
    val table = System.getenv("DAX_TABLE_NAME").takeUnless { it.isNullOrEmpty() } ?: "DaxParityTable"
    val id = UUID.randomUUID()
    val key = mapOf(
        "PK" to DaxAttributeValue.String("PACKAGE#KOTLIN#$id"),
        "SK" to DaxAttributeValue.String("ITEM#$id"),
    )
    val item = key + ("value" to DaxAttributeValue.String("created"))
    var inserted = false
    var failure: Throwable? = null
    try {
        client.putItem(DaxPutItemRequest(table, item, null, null, emptyMap(), emptyMap()))
        inserted = true
        var observed = false
        for (attempt in 0 until 20) {
            val output = client.getItem(DaxGetItemRequest(table, key, null, null, emptyMap()))
            if (output.item != null) {
                check(output.item == item) { "GetItem did not match the inserted item" }
                observed = true
                break
            }
            if (attempt < 19) delay(100)
        }
        check(observed) { "GetItem did not observe the inserted item after 20 attempts" }
    } catch (error: Throwable) {
        failure = error
        throw error
    } finally {
        // Cleanup must also run after a cancelled or possibly partial write.
        withContext(NonCancellable) {
            try {
                val output = client.deleteItem(DaxDeleteItemRequest(
                    table, key, DaxReturnValue.ALL_OLD, null, emptyMap(), emptyMap(),
                ))
                check(!inserted || output.attributes == item) {
                    "DeleteItem ALL_OLD did not match the inserted item"
                }
            } catch (error: Throwable) {
                val original = failure
                if (original != null) original.addSuppressed(error) else throw error
            }
        }
    }
}

fun main() = runBlocking {
    check(`ffiContractVersion`().isNotEmpty())
    val live = System.getenv("DAX_FFI_LIVE") == "1"
    val create = live || System.getenv("DAX_FFI_CREATE_CLIENT") == "1"
    val endpoint = if (create) System.getenv("DAX_ENDPOINT") else "127.0.0.1:8111"
    require(!endpoint.isNullOrEmpty()) { "DAX_ENDPOINT is required" }
    val config = DaxClientConfig(listOf(endpoint), System.getenv("AWS_REGION"), 60_000uL, 2u, 2u, 0uL, 70uL,
        20_000uL, 10, 4_000uL, 125uL, 30_000uL, 5_000uL, false, false,
        DaxIpDiscovery.DEFAULT, DaxLogLevel.OFF)
    if (create) {
        val client = createDaxClient(config)
        try { if (live) liveOperations(client) }
        finally {
            try { client.shutdown() } finally { client.close() }
        }
    }
    println(if (live) "Kotlin/JVM package live operations passed" else "kotlin JVM package native load ok")
}
