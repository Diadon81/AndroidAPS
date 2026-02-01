package app.aaps.pump.common.hw.rileylink.ble.operations

import android.annotation.SuppressLint
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCharacteristic
import android.os.SystemClock
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.common.hw.rileylink.ble.RileyLinkBLE
import app.aaps.pump.common.hw.rileylink.ble.data.GattAttributes.lookup
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Created by geoff on 5/26/16.
 * Updated: Fixed to use value from callback instead of deprecated characteristic.value
 * This is important for Android 13+ where characteristic.value is unreliable
 */
class CharacteristicReadOperation(
    private val aapsLogger: AAPSLogger,
    private val gatt: BluetoothGatt,
    private val characteristic: BluetoothGattCharacteristic
) : BLECommOperation() {

    @SuppressLint("MissingPermission")
    override fun execute(comm: RileyLinkBLE) {
        val readInitiated = gatt.readCharacteristic(characteristic)

        if (!readInitiated) {
            aapsLogger.error(LTag.PUMPBTCOMM, "readCharacteristic failed to initiate")
            timedOut = true
            return
        }

        try {
            val didAcquire = operationComplete.tryAcquire(
                getGattOperationTimeout_ms().toLong(),
                TimeUnit.MILLISECONDS
            )
            if (didAcquire) {
                SystemClock.sleep(1)
                // Value is now set in gattOperationCompletionCallback
                // DO NOT use characteristic.value here - it's deprecated and unreliable on Android 13+
            } else {
                aapsLogger.error(LTag.PUMPBTCOMM, "Timeout waiting for characteristic read callback")
                timedOut = true
            }
        } catch (e: InterruptedException) {
            aapsLogger.error(LTag.PUMPBTCOMM, "Interrupted waiting for characteristic read")
            interrupted = true
        }
    }

    override fun gattOperationCompletionCallback(uuid: UUID, value: ByteArray) {
        if (characteristic.uuid != uuid) {
            aapsLogger.error(LTag.PUMPBTCOMM,
                "Read callback UUID mismatch: expected ${lookup(characteristic.uuid)}, got ${lookup(uuid)}")
        }
        // Store the value from callback - this is the correct way for Android 13+
        this.value = value
        operationComplete.release()
    }
}
