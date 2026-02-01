package app.aaps.pump.common.hw.rileylink.ble.operations

import android.annotation.SuppressLint
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCharacteristic
import android.os.Build
import android.os.SystemClock
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.common.hw.rileylink.ble.RileyLinkBLE
import app.aaps.pump.common.hw.rileylink.ble.data.GattAttributes.lookup
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Created by geoff on 5/26/16.
 * Updated: Android 13+ (API 33) compatibility with new writeCharacteristic API
 */
class CharacteristicWriteOperation(
    private val aapsLogger: AAPSLogger,
    private val gatt: BluetoothGatt,
    private val characteristic: BluetoothGattCharacteristic,
    value: ByteArray?
) : BLECommOperation() {

    init {
        this.value = value
    }

    @SuppressLint("MissingPermission")
    override fun execute(comm: RileyLinkBLE) {
        val writeResult = performWrite()

        // On Android 13+, writeCharacteristic returns result code immediately
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (writeResult != BluetoothGatt.GATT_SUCCESS) {
                aapsLogger.error(LTag.PUMPBTCOMM,
                    "writeCharacteristic failed immediately with code: $writeResult")
                timedOut = true
                return
            }
        } else {
            // Legacy API returns boolean
            @Suppress("DEPRECATION")
            if (!writeResult.let { it == BluetoothGatt.GATT_SUCCESS || it == 1 }) {
                aapsLogger.error(LTag.PUMPBTCOMM, "writeCharacteristic failed to initiate")
                timedOut = true
                return
            }
        }

        // Wait for callback
        try {
            val didAcquire = operationComplete.tryAcquire(
                getGattOperationTimeout_ms().toLong(),
                TimeUnit.MILLISECONDS
            )
            if (didAcquire) {
                SystemClock.sleep(1)
            } else {
                aapsLogger.error(LTag.PUMPBTCOMM, "Timeout waiting for characteristic write callback")
                timedOut = true
            }
        } catch (e: InterruptedException) {
            aapsLogger.error(LTag.PUMPBTCOMM, "Interrupted waiting for characteristic write")
            interrupted = true
        }
    }

    @SuppressLint("MissingPermission")
    private fun performWrite(): Int {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            // Android 13+ (API 33): new API with ByteArray parameter
            gatt.writeCharacteristic(
                characteristic,
                value ?: ByteArray(0),
                BluetoothGattCharacteristic.WRITE_TYPE_DEFAULT
            )
        } else {
            // Legacy API: set value then write
            @Suppress("DEPRECATION")
            characteristic.setValue(value)
            @Suppress("DEPRECATION")
            if (gatt.writeCharacteristic(characteristic)) {
                BluetoothGatt.GATT_SUCCESS
            } else {
                BluetoothGatt.GATT_FAILURE
            }
        }
    }

    override fun gattOperationCompletionCallback(uuid: UUID, value: ByteArray) {
        if (characteristic.uuid != uuid) {
            aapsLogger.error(LTag.PUMPBTCOMM,
                "Write callback UUID mismatch: expected ${lookup(characteristic.uuid)}, got ${lookup(uuid)}")
        }
        operationComplete.release()
    }
}
