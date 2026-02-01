package app.aaps.pump.common.hw.rileylink.ble.device

import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.le.ScanCallback
import android.bluetooth.le.ScanFilter
import android.bluetooth.le.ScanResult
import android.bluetooth.le.ScanSettings
import android.os.Build
import android.os.Handler
import android.os.HandlerThread
import android.os.Message
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.utils.pump.ByteUtil
import app.aaps.pump.common.hw.rileylink.ble.RileyLinkBLE
import app.aaps.pump.common.hw.rileylink.ble.data.GattAttributes
import app.aaps.pump.common.hw.rileylink.ble.operations.BLECommOperationResult
import app.aaps.pump.common.hw.rileylink.defs.RileyLinkError
import app.aaps.pump.common.hw.rileylink.defs.RileyLinkServiceState
import app.aaps.pump.common.hw.rileylink.service.RileyLinkServiceData
import java.util.UUID
import javax.inject.Inject
import javax.inject.Singleton

/**
 * OrangeLink BLE scanning and notification handling.
 * Updated: Android 12-16 compatibility improvements for BLE scanning
 */
@Singleton
class OrangeLinkImpl @Inject constructor(
    var aapsLogger: AAPSLogger,
    var rileyLinkServiceData: RileyLinkServiceData
) {

    lateinit var rileyLinkBLE: RileyLinkBLE

    // Scan state management
    private var scanRetryCount = 0
    @Volatile
    private var isScanning = false

    companion object {
        const val SCAN_TIMEOUT_MS = 60_000  // 60 seconds
        const val MAX_SCAN_RETRIES = 3
        private const val MSG_TIMEOUT = 0x12
        private const val MSG_RETRY = 0x13
    }

    fun onCharacteristicChanged(characteristic: BluetoothGattCharacteristic, data: ByteArray) {
        if (characteristic.uuid.toString() == GattAttributes.CHARA_NOTIFICATION_ORANGE) {
            // Validate data array has enough bytes before accessing indices
            if (data.size < 7) {
                aapsLogger.error(LTag.PUMPBTCOMM,
                    "OrangeLinkImpl: onCharacteristicChanged received incomplete data, size=${data.size}, expected at least 7 bytes")
                return
            }
            val first = 0xff and data[0].toInt()
            aapsLogger.info(LTag.PUMPBTCOMM,
                "OrangeLinkImpl: onCharacteristicChanged ${ByteUtil.shortHexString(data)}=====$first")
            val fv = data[3].toString() + "." + data[4]
            val hv = data[5].toString() + "." + data[6]
            rileyLinkServiceData.versionOrangeFirmware = fv
            rileyLinkServiceData.versionOrangeHardware = hv

            aapsLogger.info(LTag.PUMPBTCOMM, "OrangeLink: Firmware: $fv, Hardware: $hv")
        }
    }

    fun resetOrangeLinkData() {
        rileyLinkServiceData.isOrange = false
        rileyLinkServiceData.versionOrangeFirmware = null
        rileyLinkServiceData.versionOrangeHardware = null
    }

    /**
     * Check if this is an OrangeLink device (has ORANGE_NOTIFICATION_SERVICE)
     */
    fun checkIsOrange(uuidService: UUID) {
        if (GattAttributes.isOrange(uuidService))
            rileyLinkServiceData.isOrange = true
    }

    fun enableNotifications(): Boolean {
        aapsLogger.info(LTag.PUMPBTCOMM, "OrangeLinkImpl::enableNotifications")
        val result: BLECommOperationResult = rileyLinkBLE.setNotificationBlocking(
            UUID.fromString(GattAttributes.SERVICE_RADIO_ORANGE),
            UUID.fromString(GattAttributes.CHARA_NOTIFICATION_ORANGE)
        )

        if (result.resultCode != BLECommOperationResult.RESULT_SUCCESS) {
            aapsLogger.error(LTag.PUMPBTCOMM, "Error setting OrangeLink notification")
            return false
        }
        return true
    }

    private fun buildScanFilters(): List<ScanFilter> {
        val address = rileyLinkServiceData.rileyLinkAddress
        if (address.isNullOrEmpty()) {
            aapsLogger.warn(LTag.PUMPBTCOMM, "No RileyLink address configured for scan filter")
            return emptyList()
        }

        return listOf(
            ScanFilter.Builder()
                .setDeviceAddress(address)
                .build()
        )
    }

    /**
     * Build scan settings with Android version-specific optimizations.
     *
     * Android 12+: SCAN_MODE_LOW_LATENCY can be throttled, use BALANCED instead
     * Android 8+: Support for legacy mode and PHY settings
     */
    @SuppressLint("MissingPermission")
    private fun buildScanSettings(): ScanSettings {
        val builder = ScanSettings.Builder()

        // Scan mode: Android 12+ throttles LOW_LATENCY in background
        when {
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.S -> {
                builder.setScanMode(ScanSettings.SCAN_MODE_BALANCED)
                aapsLogger.debug(LTag.PUMPBTCOMM, "Using SCAN_MODE_BALANCED for Android 12+")
            }
            else -> {
                builder.setScanMode(ScanSettings.SCAN_MODE_LOW_LATENCY)
            }
        }

        builder.setMatchMode(ScanSettings.MATCH_MODE_AGGRESSIVE)
        builder.setCallbackType(ScanSettings.CALLBACK_TYPE_ALL_MATCHES)
        builder.setNumOfMatches(ScanSettings.MATCH_NUM_ONE_ADVERTISEMENT)

        // Android 8+: PHY and legacy settings
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            builder.setLegacy(true)  // OrangeLink uses legacy advertising
            builder.setPhy(ScanSettings.PHY_LE_ALL_SUPPORTED)
        }

        return builder.build()
    }

    /**
     * Start BLE scan for OrangeLink device.
     */
    @SuppressLint("MissingPermission")
    fun startScan() {
        if (isScanning) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "Scan already in progress, skipping")
            return
        }

        // Verify we have an address to scan for
        val address = rileyLinkServiceData.rileyLinkAddress
        if (address.isNullOrEmpty()) {
            aapsLogger.error(LTag.PUMPBTCOMM, "Cannot start scan - no RileyLink address configured")
            rileyLinkServiceData.setServiceState(
                RileyLinkServiceState.BluetoothError,
                RileyLinkError.NoBluetoothAdapter
            )
            return
        }

        try {
            stopScanInternal()

            aapsLogger.debug(LTag.PUMPBTCOMM,
                "Starting BLE scan (attempt ${scanRetryCount + 1}/${MAX_SCAN_RETRIES + 1})")
            isScanning = true

            handler.sendEmptyMessageDelayed(MSG_TIMEOUT, SCAN_TIMEOUT_MS.toLong())

            val scanner = rileyLinkBLE.bluetoothAdapter?.bluetoothLeScanner
            if (scanner == null) {
                aapsLogger.error(LTag.PUMPBTCOMM, "BluetoothLeScanner is null - Bluetooth may be off")
                handleScanError(ScanCallback.SCAN_FAILED_INTERNAL_ERROR)
                return
            }

            val filters = buildScanFilters()
            val settings = buildScanSettings()

            scanner.startScan(filters, settings, scanCallback)
            aapsLogger.info(LTag.PUMPBTCOMM,
                "BLE scan started for address: $address")

        } catch (e: Exception) {
            aapsLogger.error(LTag.PUMPBTCOMM, "startScan exception: ${e.message}", e)
            handleScanError(ScanCallback.SCAN_FAILED_INTERNAL_ERROR)
        }
    }

    /**
     * Start scan specifically for reconnection after disconnect.
     * Resets retry counter for fresh reconnection attempt.
     */
    fun startReconnectScan() {
        aapsLogger.info(LTag.PUMPBTCOMM, "Starting reconnect scan for OrangeLink")
        scanRetryCount = 0
        startScan()
    }

    private val scanCallback: ScanCallback = object : ScanCallback() {

        @SuppressLint("MissingPermission")
        override fun onScanResult(callbackType: Int, result: ScanResult) {
            super.onScanResult(callbackType, result)

            val address = result.device.address
            val targetAddress = rileyLinkServiceData.rileyLinkAddress

            aapsLogger.debug(LTag.PUMPBTCOMM, "Scan result: $address (looking for $targetAddress)")

            if (address.equals(targetAddress, ignoreCase = true)) {
                aapsLogger.info(LTag.PUMPBTCOMM, "Found OrangeLink device: $address, RSSI: ${result.rssi}")
                scanRetryCount = 0  // Reset on success
                stopScan()

                rileyLinkBLE.rileyLinkDevice = result.device
                rileyLinkBLE.connectGattInternal()
            }
        }

        override fun onBatchScanResults(results: List<ScanResult>) {
            super.onBatchScanResults(results)
            aapsLogger.debug(LTag.PUMPBTCOMM, "Batch scan results: ${results.size} devices")
            results.forEach { result ->
                onScanResult(ScanSettings.CALLBACK_TYPE_ALL_MATCHES, result)
            }
        }

        override fun onScanFailed(errorCode: Int) {
            super.onScanFailed(errorCode)
            aapsLogger.error(LTag.PUMPBTCOMM, "Scan failed: ${getScanErrorString(errorCode)}")
            handleScanError(errorCode)
        }
    }

    /**
     * Handle scan errors with retry logic.
     * Some errors should not be retried (e.g., app registration failed).
     */
    private fun handleScanError(errorCode: Int) {
        stopScanInternal()

        // Determine if retry is appropriate
        val shouldRetry = when (errorCode) {
            ScanCallback.SCAN_FAILED_APPLICATION_REGISTRATION_FAILED -> {
                aapsLogger.error(LTag.PUMPBTCOMM, "App registration failed - cannot retry")
                false
            }
            ScanCallback.SCAN_FAILED_FEATURE_UNSUPPORTED -> {
                aapsLogger.error(LTag.PUMPBTCOMM, "BLE scanning not supported")
                false
            }
            ScanCallback.SCAN_FAILED_ALREADY_STARTED -> {
                // This shouldn't happen with our isScanning guard, but try to recover
                aapsLogger.warn(LTag.PUMPBTCOMM, "Scan already started - attempting recovery")
                true
            }
            else -> true
        }

        if (shouldRetry && scanRetryCount < MAX_SCAN_RETRIES) {
            scanRetryCount++
            val delay = 2000L * scanRetryCount  // 2s, 4s, 6s
            aapsLogger.info(LTag.PUMPBTCOMM,
                "Scheduling scan retry $scanRetryCount/$MAX_SCAN_RETRIES in ${delay}ms")
            handler.sendEmptyMessageDelayed(MSG_RETRY, delay)
        } else {
            scanRetryCount = 0
            aapsLogger.error(LTag.PUMPBTCOMM, "Scan failed after all retries, starting background reconnect")
            rileyLinkServiceData.setServiceState(
                RileyLinkServiceState.BluetoothError,
                RileyLinkError.RileyLinkUnreachable
            )
            // Start background reconnect to recover when device comes back in range
            rileyLinkBLE.startBackgroundReconnect()
        }
    }

    private fun getScanErrorString(errorCode: Int): String = when (errorCode) {
        ScanCallback.SCAN_FAILED_ALREADY_STARTED -> "ALREADY_STARTED"
        ScanCallback.SCAN_FAILED_APPLICATION_REGISTRATION_FAILED -> "APP_REGISTRATION_FAILED"
        ScanCallback.SCAN_FAILED_INTERNAL_ERROR -> "INTERNAL_ERROR"
        ScanCallback.SCAN_FAILED_FEATURE_UNSUPPORTED -> "FEATURE_UNSUPPORTED"
        5 -> "OUT_OF_HARDWARE_RESOURCES"  // Android 13+
        6 -> "SCANNING_TOO_FREQUENTLY"    // Android 13+
        else -> "UNKNOWN ($errorCode)"
    }

    // Store reference to HandlerThread for proper cleanup
    private val handlerThread: HandlerThread = HandlerThread(
        this::class.java.simpleName + "Handler"
    ).also { it.start() }

    private val handler: Handler = object : Handler(handlerThread.looper) {
        override fun handleMessage(msg: Message) {
            when (msg.what) {
                MSG_TIMEOUT -> {
                    aapsLogger.warn(LTag.PUMPBTCOMM, "Scan timeout after ${SCAN_TIMEOUT_MS}ms")
                    handleScanError(ScanCallback.SCAN_FAILED_INTERNAL_ERROR)
                }
                MSG_RETRY -> {
                    aapsLogger.debug(LTag.PUMPBTCOMM, "Executing scan retry")
                    startScan()
                }
            }
        }
    }

    /**
     * Clean up resources. Call this when OrangeLinkImpl is no longer needed.
     */
    fun cleanup() {
        stopScan()
        handlerThread.quitSafely()
    }

    /**
     * Stop scan and reset retry counter.
     */
    @SuppressLint("MissingPermission")
    fun stopScan() {
        aapsLogger.debug(LTag.PUMPBTCOMM, "Stopping scan (public)")
        stopScanInternal()
        scanRetryCount = 0
    }

    /**
     * Internal stop without resetting retry counter.
     */
    @SuppressLint("MissingPermission")
    private fun stopScanInternal() {
        handler.removeMessages(MSG_TIMEOUT)
        handler.removeMessages(MSG_RETRY)
        isScanning = false

        try {
            if (isBluetoothAvailable()) {
                rileyLinkBLE.bluetoothAdapter?.bluetoothLeScanner?.stopScan(scanCallback)
                aapsLogger.debug(LTag.PUMPBTCOMM, "Scan stopped")
            }
        } catch (e: Exception) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "stopScan exception (may be expected): ${e.message}")
        }
    }

    private fun isBluetoothAvailable(): Boolean =
        rileyLinkBLE.bluetoothAdapter?.isEnabled == true &&
        rileyLinkBLE.bluetoothAdapter?.state == BluetoothAdapter.STATE_ON
}
