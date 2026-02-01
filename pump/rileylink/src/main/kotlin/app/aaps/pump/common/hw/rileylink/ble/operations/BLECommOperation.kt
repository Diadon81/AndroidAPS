package app.aaps.pump.common.hw.rileylink.ble.operations

import app.aaps.pump.common.hw.rileylink.ble.RileyLinkBLE
import java.util.UUID
import java.util.concurrent.Semaphore

/**
 * Created by geoff on 5/26/16.
 */
abstract class BLECommOperation {

    var timedOut: Boolean = false
    var interrupted: Boolean = false
    var value: ByteArray? = null
    var operationComplete: Semaphore = Semaphore(0, true)

    // This is to be run on the main thread
    abstract fun execute(comm: RileyLinkBLE)

    open fun gattOperationCompletionCallback(uuid: UUID, value: ByteArray) {
    }

    // Timeout must be less than OrangeLink CONN_SUP_TIMEOUT (4000ms)
    // to avoid OrangeLink dropping connection while we're still waiting
    fun getGattOperationTimeout_ms(): Int = 3500
}
