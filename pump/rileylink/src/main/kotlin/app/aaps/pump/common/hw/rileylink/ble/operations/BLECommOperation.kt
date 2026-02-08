package app.aaps.pump.common.hw.rileylink.ble.operations

import app.aaps.pump.common.hw.rileylink.ble.RileyLinkBLE
import java.util.UUID
import java.util.concurrent.Semaphore

/**
 * Created by geoff on 5/26/16.
 */
abstract class BLECommOperation(
    private val gattOperationTimeoutMs: Int = DEFAULT_GATT_OPERATION_TIMEOUT_MS
) {

    companion object {
        // Default timeout for RileyLink/EmaLink (BLE113 module) - no explicit supervision timeout
        const val DEFAULT_GATT_OPERATION_TIMEOUT_MS = 22_000

        // OrangeLink timeout - generous to handle loaded BLE stack (CONN_SUP_TIMEOUT=5000ms)
        const val ORANGELINK_GATT_OPERATION_TIMEOUT_MS = 10_000
    }

    @Volatile var timedOut: Boolean = false
    @Volatile var interrupted: Boolean = false
    @Volatile var value: ByteArray? = null
    var operationComplete: Semaphore = Semaphore(0, true)

    // This is to be run on the main thread
    abstract fun execute(comm: RileyLinkBLE)

    open fun gattOperationCompletionCallback(uuid: UUID, value: ByteArray) {
    }

    fun getGattOperationTimeout_ms(): Int = gattOperationTimeoutMs
}
