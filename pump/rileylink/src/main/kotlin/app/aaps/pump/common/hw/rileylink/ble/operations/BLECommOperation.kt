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

        // OrangeLink timeout - must be less than CONN_SUP_TIMEOUT (4000ms)
        const val ORANGELINK_GATT_OPERATION_TIMEOUT_MS = 3_500
    }

    var timedOut: Boolean = false
    var interrupted: Boolean = false
    var value: ByteArray? = null
    var operationComplete: Semaphore = Semaphore(0, true)

    // This is to be run on the main thread
    abstract fun execute(comm: RileyLinkBLE)

    open fun gattOperationCompletionCallback(uuid: UUID, value: ByteArray) {
    }

    fun getGattOperationTimeout_ms(): Int = gattOperationTimeoutMs
}
