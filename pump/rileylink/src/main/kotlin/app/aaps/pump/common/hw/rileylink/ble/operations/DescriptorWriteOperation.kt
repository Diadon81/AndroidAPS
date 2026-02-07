package app.aaps.pump.common.hw.rileylink.ble.operations

import android.annotation.SuppressLint
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattDescriptor
import android.os.Build
import android.os.SystemClock
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.pump.common.hw.rileylink.ble.RileyLinkBLE
import java.util.UUID
import java.util.concurrent.TimeUnit

/**
 * Created by geoff on 5/26/16.
 * Updated: Android 13+ (API 33) compatibility with new writeDescriptor API
 */
class DescriptorWriteOperation(
    private val aapsLogger: AAPSLogger,
    private val gatt: BluetoothGatt,
    private val descriptor: BluetoothGattDescriptor,
    value: ByteArray,
    gattOperationTimeoutMs: Int = DEFAULT_GATT_OPERATION_TIMEOUT_MS
) : BLECommOperation(gattOperationTimeoutMs) {

    init {
        this.value = value
    }

    @SuppressLint("MissingPermission")
    override fun execute(comm: RileyLinkBLE) {
        val writeResult = performWrite()

        // On Android 13+, writeDescriptor returns result code immediately
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            if (writeResult != BluetoothGatt.GATT_SUCCESS) {
                aapsLogger.error(LTag.PUMPBTCOMM,
                    "writeDescriptor failed immediately with code: $writeResult")
                timedOut = true
                return
            }
        } else {
            // Legacy API returns boolean (mapped to GATT_SUCCESS/GATT_FAILURE)
            if (writeResult != BluetoothGatt.GATT_SUCCESS) {
                aapsLogger.error(LTag.PUMPBTCOMM, "writeDescriptor failed to initiate")
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
                aapsLogger.error(LTag.PUMPBTCOMM, "Timeout waiting for descriptor write callback")
                timedOut = true
            }
        } catch (e: InterruptedException) {
            aapsLogger.error(LTag.PUMPBTCOMM, "Interrupted waiting for descriptor write")
            interrupted = true
        }
    }

    @SuppressLint("MissingPermission")
    private fun performWrite(): Int {
        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            // Android 13+ (API 33): new API with ByteArray parameter
            gatt.writeDescriptor(descriptor, value ?: ByteArray(0))
        } else {
            // Legacy API: set value then write
            @Suppress("DEPRECATION")
            descriptor.setValue(value)
            @Suppress("DEPRECATION")
            if (gatt.writeDescriptor(descriptor)) {
                BluetoothGatt.GATT_SUCCESS
            } else {
                BluetoothGatt.GATT_FAILURE
            }
        }
    }

    override fun gattOperationCompletionCallback(uuid: UUID, value: ByteArray) {
        operationComplete.release()
    }
}
