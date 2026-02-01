package app.aaps.pump.common.hw.rileylink.ble

import android.annotation.SuppressLint
import android.bluetooth.BluetoothAdapter
import android.bluetooth.BluetoothDevice
import android.bluetooth.BluetoothGatt
import android.bluetooth.BluetoothGattCallback
import android.bluetooth.BluetoothGattCharacteristic
import android.bluetooth.BluetoothGattDescriptor
import android.bluetooth.BluetoothGattService
import android.bluetooth.BluetoothManager
import android.bluetooth.BluetoothProfile
import android.content.Context
import android.content.pm.PackageManager
import android.os.Build
import android.os.Handler
import android.os.Looper
import android.os.SystemClock
import androidx.core.content.ContextCompat
import app.aaps.core.interfaces.configuration.Config
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.keys.interfaces.Preferences
import app.aaps.core.utils.pump.ByteUtil
import app.aaps.core.utils.pump.ThreadUtil
import app.aaps.pump.common.hw.rileylink.RileyLinkConst
import app.aaps.pump.common.hw.rileylink.RileyLinkUtil
import app.aaps.pump.common.hw.rileylink.ble.data.GattAttributes
import app.aaps.pump.common.hw.rileylink.ble.device.OrangeLinkImpl
import app.aaps.pump.common.hw.rileylink.ble.operations.BLECommOperation
import app.aaps.pump.common.hw.rileylink.ble.operations.BLECommOperationResult
import app.aaps.pump.common.hw.rileylink.ble.operations.CharacteristicReadOperation
import app.aaps.pump.common.hw.rileylink.ble.operations.CharacteristicWriteOperation
import app.aaps.pump.common.hw.rileylink.ble.operations.DescriptorWriteOperation
import app.aaps.pump.common.hw.rileylink.defs.RileyLinkError
import app.aaps.pump.common.hw.rileylink.defs.RileyLinkServiceState
import app.aaps.pump.common.hw.rileylink.keys.RileyLinkStringKey
import app.aaps.pump.common.hw.rileylink.keys.RileylinkBooleanPreferenceKey
import app.aaps.pump.common.hw.rileylink.service.RileyLinkServiceData
import org.apache.commons.lang3.StringUtils
import java.util.Locale
import java.util.UUID
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.concurrent.withLock

/**
 * Created by geoff on 5/26/16.
 * Added: State handling, configuration of RF for different configuration ranges, connection handling
 * Updated: Android 12-16 compatibility improvements
 */
@Singleton
class RileyLinkBLE @Inject constructor(
    private val context: Context,
    private val aapsLogger: AAPSLogger,
    private val rileyLinkServiceData: RileyLinkServiceData,
    private val rileyLinkUtil: RileyLinkUtil,
    private val preferences: Preferences,
    private val orangeLink: OrangeLinkImpl,
    private val config: Config
) {

    private val gattDebugEnabled = true
    @Volatile
    private var manualDisconnect = false

    val bluetoothAdapter: BluetoothAdapter?
        get() = (context.getSystemService(Context.BLUETOOTH_SERVICE) as BluetoothManager?)?.adapter

    private val bluetoothGattCallback: BluetoothGattCallback
    var rileyLinkDevice: BluetoothDevice? = null

    // Lock for thread-safe access to bluetoothConnectionGatt and mCurrentOperation
    private val gattLock = ReentrantLock()
    private var bluetoothConnectionGatt: BluetoothGatt? = null
    @Volatile
    private var mCurrentOperation: BLECommOperation? = null
    private val gattOperationSema = Semaphore(1, true)
    private var radioResponseCountNotified: Runnable? = null
    @Volatile
    var isConnected = false
        private set

    // ===== Android 12-16 Improvements =====

    // Handler for main thread operations
    private val mainHandler = Handler(Looper.getMainLooper())

    // AutoConnect fallback mechanism
    private var lastDisconnectTime: Long = 0
    private val autoConnectLock = Any()
    @Volatile
    private var autoConnectCheckRunnable: Runnable? = null

    // GATT 133 retry mechanism - use AtomicInteger for thread-safe increment
    private val gatt133RetryCount = AtomicInteger(0)

    // Callback for reconnect in scanning mode
    var onReconnectNeeded: (() -> Unit)? = null

    // ===== Background Reconnect Mechanism =====
    // Periodically attempts to reconnect when in error state
    // This ensures recovery when user returns to BLE range
    @Volatile
    private var backgroundReconnectRunnable: Runnable? = null
    private val backgroundReconnectLock = Any()
    private var backgroundReconnectAttempt = 0

    // ===== Service Discovery Timeout =====
    // On Android 12+ service discovery can hang without callback
    @Volatile
    private var serviceDiscoveryTimeoutRunnable: Runnable? = null
    private val serviceDiscoveryLock = Any()

    companion object {
        // OrangeLink supervision timeout is 4 seconds, react faster
        private const val AUTO_CONNECT_TIMEOUT_MS = 5_000L
        private const val MAX_GATT_133_RETRIES = 3
        private const val GATT_ERROR_133 = 133
        private const val PREFERRED_MTU = 185  // Default BLE MTU is 23, max is 517

        // Background reconnect: when all retries exhausted but device might come back in range
        // Uses exponential backoff: 10s, 20s, 30s (capped) - balanced for medical device responsiveness
        private const val BACKGROUND_RECONNECT_INITIAL_DELAY_MS = 10_000L
        private const val BACKGROUND_RECONNECT_MAX_DELAY_MS = 30_000L

        // Service discovery timeout - if onServicesDiscovered not called within this time, retry
        // Reduced to 8s for faster recovery with OrangeLink
        private const val SERVICE_DISCOVERY_TIMEOUT_MS = 8_000L

        // OrangeLink connection parameters for reference:
        // MIN_CONN_INTERVAL: 50ms, MAX_CONN_INTERVAL: 100ms
        // SLAVE_LATENCY: 0, CONN_SUP_TIMEOUT: 5000ms
    }

    @Inject fun onInit() {
        orangeLink.rileyLinkBLE = this
    }

    private fun isAnyRileyLinkServiceFound(service: BluetoothGattService): Boolean {
        val found = GattAttributes.isRileyLink(service.uuid)
        if (found) return true
        else
            for (serviceI in service.includedServices) {
                if (isAnyRileyLinkServiceFound(serviceI)) return true
                orangeLink.checkIsOrange(serviceI.uuid)
            }
        return false
    }

    fun debugService(service: BluetoothGattService, indentCount: Int, stringBuilder: StringBuilder) {
        val indentString = StringUtils.repeat(' ', indentCount)
        if (gattDebugEnabled) {
            val uuidServiceString = service.uuid.toString()

            stringBuilder.append(indentString)
            stringBuilder.append(GattAttributes.lookup(uuidServiceString, "Unknown service"))
            stringBuilder.append(" ($uuidServiceString)")
            for (character in service.characteristics) {
                val uuidCharacteristicString = character.uuid.toString()
                stringBuilder.append("\n    ")
                stringBuilder.append(indentString)
                stringBuilder.append(" - " + GattAttributes.lookup(uuidCharacteristicString, "Unknown Characteristic"))
                stringBuilder.append(" ($uuidCharacteristicString)")
            }
            stringBuilder.append("\n\n")

            for (serviceI in service.includedServices) {
                debugService(serviceI, indentCount + 4, stringBuilder)
            }
        }
    }

    fun registerRadioResponseCountNotification(notifier: Runnable?) {
        radioResponseCountNotified = notifier
    }

    @SuppressLint("MissingPermission")
    fun discoverServices(): Boolean {
        val gatt = gattLock.withLock { bluetoothConnectionGatt }
        if (gatt == null) {
            aapsLogger.error(LTag.PUMPBTCOMM, "discoverServices: bluetoothConnectionGatt is null!")
            return false
        }

        return if (gatt.discoverServices()) {
            aapsLogger.warn(LTag.PUMPBTCOMM, "Starting to discover GATT Services.")
            // Schedule timeout - if onServicesDiscovered not called, retry connection
            scheduleServiceDiscoveryTimeout()
            true
        } else {
            aapsLogger.error(LTag.PUMPBTCOMM, "Cannot discover GATT Services.")
            false
        }
    }

    private fun scheduleServiceDiscoveryTimeout() {
        synchronized(serviceDiscoveryLock) {
            cancelServiceDiscoveryTimeout()

            val runnable = Runnable {
                if (!isConnected) {
                    aapsLogger.error(LTag.PUMPBTCOMM,
                        "Service discovery timeout after ${SERVICE_DISCOVERY_TIMEOUT_MS}ms - retrying connection")
                    rileyLinkServiceData.setServiceState(
                        RileyLinkServiceState.BluetoothError,
                        RileyLinkError.RileyLinkUnreachable
                    )
                    // Close and retry
                    close()
                    startBackgroundReconnect()
                }
            }
            serviceDiscoveryTimeoutRunnable = runnable
            mainHandler.postDelayed(runnable, SERVICE_DISCOVERY_TIMEOUT_MS)
            aapsLogger.debug(LTag.PUMPBTCOMM, "Service discovery timeout scheduled in ${SERVICE_DISCOVERY_TIMEOUT_MS}ms")
        }
    }

    private fun cancelServiceDiscoveryTimeout() {
        synchronized(serviceDiscoveryLock) {
            serviceDiscoveryTimeoutRunnable?.let {
                mainHandler.removeCallbacks(it)
                aapsLogger.debug(LTag.PUMPBTCOMM, "Service discovery timeout cancelled")
            }
            serviceDiscoveryTimeoutRunnable = null
        }
    }

    fun enableNotifications(): Boolean {
        val result = setNotificationBlocking(
            UUID.fromString(GattAttributes.SERVICE_RADIO),
            UUID.fromString(GattAttributes.CHARA_RADIO_RESPONSE_COUNT)
        )
        if (result.resultCode != BLECommOperationResult.RESULT_SUCCESS) {
            aapsLogger.error(LTag.PUMPBTCOMM, "Error setting response count notification")
            return false
        }
        return if (rileyLinkServiceData.isOrange) orangeLink.enableNotifications()
        else true
    }

    fun findRileyLink(rileyLinkAddress: String) {
        aapsLogger.debug(LTag.PUMPBTCOMM, "RileyLink address: $rileyLinkAddress")
        val useScanning = preferences.get(RileylinkBooleanPreferenceKey.OrangeUseScanning)
        if (useScanning) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "Start scan for OrangeLink device.")
            orangeLink.startScan()
        } else {
            rileyLinkDevice = bluetoothAdapter?.getRemoteDevice(rileyLinkAddress)
            if (rileyLinkDevice != null) connectGattInternal()
            else aapsLogger.error(LTag.PUMPBTCOMM, "RileyLink device not found with address: $rileyLinkAddress")
        }
    }

    fun connectGatt() {
        val useScanning = preferences.get(RileylinkBooleanPreferenceKey.OrangeUseScanning)
        if (useScanning) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "Start scan for OrangeLink device.")
            orangeLink.startScan()
        } else {
            connectGattInternal()
        }
    }

    /**
     * Connect to GATT with Android version-specific optimizations.
     *
     * Android 12-16 improvements:
     * - Uses TRANSPORT_LE for explicit BLE transport (Android 6+)
     * - Uses PHY_LE_1M for better compatibility (Android 8+)
     * - Uses Handler for callback thread control (Android 8+)
     */
    @SuppressLint("HardwareIds", "MissingPermission")
    fun connectGattInternal() {
        if (rileyLinkDevice == null) {
            aapsLogger.error(LTag.PUMPBTCOMM, "RileyLink device is null, can't do connectGatt.")
            return
        }

        if (!hasBluetoothConnectPermission()) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "No BLUETOOTH_CONNECT permission")
            return
        }

        // Reset state for new connection
        manualDisconnect = false
        cancelAutoConnectCheck()

        val gatt = createGattConnection()

        gattLock.withLock {
            bluetoothConnectionGatt = gatt
        }

        if (gatt == null) {
            aapsLogger.error(LTag.PUMPBTCOMM, "Failed to connect to Bluetooth Low Energy device at ${bluetoothAdapter?.address}")
            // Start background reconnect since GATT creation failed
            startBackgroundReconnect()
        } else {
            if (gattDebugEnabled) aapsLogger.debug(LTag.PUMPBTCOMM, "GATT connection initiated (Android ${Build.VERSION.SDK_INT})")
            updateDeviceInfo(gatt)
            // Schedule timeout for initial connection - critical for Android 12+
            // Without this, autoConnect=true can wait forever if device is out of range
            scheduleAutoConnectCheck()
        }
    }

    /**
     * Create GATT connection with version-appropriate parameters.
     */
    @SuppressLint("MissingPermission")
    private fun createGattConnection(): BluetoothGatt? {
        return when {
            // Android 8+ (API 26): TRANSPORT_LE + PHY + Handler
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.O -> {
                aapsLogger.debug(LTag.PUMPBTCOMM, "Using Android 8+ connectGatt with TRANSPORT_LE and Handler")
                rileyLinkDevice?.connectGatt(
                    context,
                    true,  // autoConnect
                    bluetoothGattCallback,
                    BluetoothDevice.TRANSPORT_LE,
                    BluetoothDevice.PHY_LE_1M_MASK,
                    mainHandler
                )
            }
            // Android 6+ (API 23): TRANSPORT_LE
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.M -> {
                aapsLogger.debug(LTag.PUMPBTCOMM, "Using Android 6+ connectGatt with TRANSPORT_LE")
                rileyLinkDevice?.connectGatt(
                    context,
                    true,
                    bluetoothGattCallback,
                    BluetoothDevice.TRANSPORT_LE
                )
            }
            // Legacy
            else -> {
                aapsLogger.debug(LTag.PUMPBTCOMM, "Using legacy connectGatt")
                rileyLinkDevice?.connectGatt(context, true, bluetoothGattCallback)
            }
        }
    }

    /**
     * Check for BLUETOOTH_CONNECT permission (required on Android 12+).
     */
    private fun hasBluetoothConnectPermission(): Boolean {
        if (!config.PUMPDRIVERS) return true

        return if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.S) {
            ContextCompat.checkSelfPermission(
                context,
                "android.permission.BLUETOOTH_CONNECT"
            ) == PackageManager.PERMISSION_GRANTED
        } else {
            true
        }
    }

    @SuppressLint("MissingPermission")
    private fun updateDeviceInfo(gatt: BluetoothGatt) {
        gatt.device?.name?.let { deviceName ->
            if (StringUtils.isNotEmpty(deviceName)) {
                preferences.put(RileyLinkStringKey.Name, deviceName)
            } else {
                preferences.remove(RileyLinkStringKey.Name)
            }
            rileyLinkServiceData.rileyLinkName = deviceName
            rileyLinkServiceData.rileyLinkAddress = gatt.device?.address
        }
    }

    // ===== AutoConnect Fallback Mechanism =====

    /**
     * Schedule a check to verify autoConnect is working.
     * If connection is not restored within timeout, force reconnection.
     */
    private fun scheduleAutoConnectCheck() {
        synchronized(autoConnectLock) {
            cancelAutoConnectCheckInternal()

            val useScanning = preferences.get(RileylinkBooleanPreferenceKey.OrangeUseScanning)

            val runnable = Runnable {
                if (!isConnected && !manualDisconnect) {
                    aapsLogger.warn(LTag.PUMPBTCOMM, "AutoConnect timeout after ${AUTO_CONNECT_TIMEOUT_MS}ms")

                    if (useScanning) {
                        // Scanning mode: close GATT and trigger rescan
                        aapsLogger.info(LTag.PUMPBTCOMM, "Scanning mode: triggering reconnect scan")
                        close()
                        // onReconnectNeeded will trigger scan which has its own retry logic
                        // If scan fails completely, it will start background reconnect
                        onReconnectNeeded?.invoke() ?: startBackgroundReconnect()
                    } else {
                        // MAC mode: force direct reconnection
                        aapsLogger.info(LTag.PUMPBTCOMM, "MAC mode: forcing reconnect")
                        forceReconnect()
                    }
                }
            }
            autoConnectCheckRunnable = runnable

            mainHandler.postDelayed(runnable, AUTO_CONNECT_TIMEOUT_MS)
            aapsLogger.debug(LTag.PUMPBTCOMM, "Scheduled autoConnect check in ${AUTO_CONNECT_TIMEOUT_MS}ms")
        }
    }

    private fun cancelAutoConnectCheck() {
        synchronized(autoConnectLock) {
            cancelAutoConnectCheckInternal()
        }
    }

    private fun cancelAutoConnectCheckInternal() {
        autoConnectCheckRunnable?.let {
            mainHandler.removeCallbacks(it)
            aapsLogger.debug(LTag.PUMPBTCOMM, "Cancelled autoConnect check")
        }
        autoConnectCheckRunnable = null
    }

    private fun forceReconnect() {
        close()
        mainHandler.postDelayed({
            if (!manualDisconnect && rileyLinkDevice != null) {
                aapsLogger.info(LTag.PUMPBTCOMM, "Attempting forced reconnection")
                connectGattInternal()
            }
        }, 1000)
    }

    // ===== Background Reconnect - recovery when returning to BLE range =====

    /**
     * Start background reconnect attempts.
     * Called when all immediate retries are exhausted but we want to keep trying
     * in case the user returns to BLE range (e.g., came back home).
     */
    fun startBackgroundReconnect() {
        synchronized(backgroundReconnectLock) {
            if (backgroundReconnectRunnable != null) {
                aapsLogger.debug(LTag.PUMPBTCOMM, "Background reconnect already running")
                return
            }

            if (manualDisconnect) {
                aapsLogger.debug(LTag.PUMPBTCOMM, "Manual disconnect - not starting background reconnect")
                return
            }

            backgroundReconnectAttempt = 0
            scheduleBackgroundReconnect()
        }
    }

    private fun scheduleBackgroundReconnect() {
        synchronized(backgroundReconnectLock) {
            if (manualDisconnect || isConnected) {
                stopBackgroundReconnect()
                return
            }

            // Exponential backoff: 30s, 60s, 120s, then cap at 120s
            val delay = (BACKGROUND_RECONNECT_INITIAL_DELAY_MS * (1 shl backgroundReconnectAttempt.coerceAtMost(2)))
                .coerceAtMost(BACKGROUND_RECONNECT_MAX_DELAY_MS)

            val runnable = Runnable {
                attemptBackgroundReconnect()
            }
            backgroundReconnectRunnable = runnable

            mainHandler.postDelayed(runnable, delay)
            aapsLogger.info(LTag.PUMPBTCOMM,
                "Background reconnect scheduled in ${delay / 1000}s (attempt ${backgroundReconnectAttempt + 1})")
        }
    }

    private fun attemptBackgroundReconnect() {
        if (manualDisconnect) {
            stopBackgroundReconnect()
            return
        }

        if (isConnected) {
            aapsLogger.info(LTag.PUMPBTCOMM, "Already connected - stopping background reconnect")
            stopBackgroundReconnect()
            return
        }

        aapsLogger.info(LTag.PUMPBTCOMM, "Background reconnect attempt ${backgroundReconnectAttempt + 1}")
        backgroundReconnectAttempt++

        val useScanning = preferences.get(RileylinkBooleanPreferenceKey.OrangeUseScanning)
        if (useScanning) {
            // Scanning mode: trigger rescan
            onReconnectNeeded?.invoke()
        } else {
            // MAC mode: attempt direct connection
            if (rileyLinkDevice != null) {
                connectGattInternal()
            } else {
                // Try to get device from stored address
                rileyLinkServiceData.rileyLinkAddress?.let { address ->
                    rileyLinkDevice = bluetoothAdapter?.getRemoteDevice(address)
                    if (rileyLinkDevice != null) {
                        connectGattInternal()
                    }
                }
            }
        }

        // Schedule next attempt (will be cancelled if connection succeeds)
        synchronized(backgroundReconnectLock) {
            backgroundReconnectRunnable = null
            scheduleBackgroundReconnect()
        }
    }

    /**
     * Stop background reconnect attempts.
     * Called when connection is restored or manual disconnect is requested.
     */
    fun stopBackgroundReconnect() {
        synchronized(backgroundReconnectLock) {
            backgroundReconnectRunnable?.let {
                mainHandler.removeCallbacks(it)
                aapsLogger.debug(LTag.PUMPBTCOMM, "Background reconnect stopped")
            }
            backgroundReconnectRunnable = null
            backgroundReconnectAttempt = 0
        }
    }

    // ===== Connection Optimization =====

    /**
     * Request higher MTU for better throughput (Android 5+).
     * Default BLE MTU is 23 bytes, we request more for efficiency.
     */
    @SuppressLint("MissingPermission")
    private fun requestMtu() {
        gattLock.withLock {
            bluetoothConnectionGatt?.requestMtu(PREFERRED_MTU)
        }
    }

    /**
     * Request high priority connection for lower latency (Android 5+).
     * Important for time-sensitive pump communication.
     */
    @SuppressLint("MissingPermission")
    private fun requestHighPriority() {
        gattLock.withLock {
            bluetoothConnectionGatt?.requestConnectionPriority(BluetoothGatt.CONNECTION_PRIORITY_HIGH)
        }
    }

    // ===== GATT 133 Retry Mechanism =====

    /**
     * Handle GATT error 133 with exponential backoff retry.
     */
    private fun handleGatt133Error() {
        val retryCount = gatt133RetryCount.incrementAndGet()
        aapsLogger.error(LTag.PUMPBTCOMM, "GATT 133 error (attempt $retryCount/$MAX_GATT_133_RETRIES)")

        close()

        if (retryCount < MAX_GATT_133_RETRIES) {
            // Exponential backoff: 1s, 2s, 4s
            val delay = (1000L * (1 shl (retryCount - 1))).coerceAtMost(5000L)
            aapsLogger.info(LTag.PUMPBTCOMM, "Retrying connection in ${delay}ms")

            mainHandler.postDelayed({
                if (!manualDisconnect) {
                    val useScanning = preferences.get(RileylinkBooleanPreferenceKey.OrangeUseScanning)
                    if (useScanning) {
                        onReconnectNeeded?.invoke()
                    } else {
                        connectGattInternal()
                    }
                }
            }, delay)
        } else {
            gatt133RetryCount.set(0)
            aapsLogger.error(LTag.PUMPBTCOMM, "GATT 133 error: max retries exceeded, starting background reconnect")
            rileyLinkServiceData.setServiceState(
                RileyLinkServiceState.RileyLinkError,
                RileyLinkError.RileyLinkUnreachable
            )
            // Start background reconnect to recover when device comes back in range
            startBackgroundReconnect()
        }
    }

    @SuppressLint("MissingPermission")
    fun disconnect() {
        isConnected = false
        manualDisconnect = true
        cancelAutoConnectCheck()
        cancelServiceDiscoveryTimeout()
        stopBackgroundReconnect()
        aapsLogger.warn(LTag.PUMPBTCOMM, "Closing GATT connection (manual)")
        gattLock.withLock {
            bluetoothConnectionGatt?.disconnect()
        }
    }

    @SuppressLint("MissingPermission")
    fun close() {
        cancelAutoConnectCheck()
        cancelServiceDiscoveryTimeout()
        gattLock.withLock {
            bluetoothConnectionGatt?.close()
            bluetoothConnectionGatt = null
        }
    }

    fun resetConnection() {
        aapsLogger.warn(LTag.PUMPBTCOMM, "Resetting BLE connection state")
        isConnected = false
        cancelAutoConnectCheck()
        stopBackgroundReconnect()
        gattLock.withLock {
            mCurrentOperation = null
            gattOperationSema.drainPermits()
            gattOperationSema.release()
        }
        close()
    }

    @SuppressLint("MissingPermission")
    fun setNotificationBlocking(serviceUUID: UUID?, charaUUID: UUID?): BLECommOperationResult {
        val retValue = BLECommOperationResult()
        val gatt = gattLock.withLock { bluetoothConnectionGatt }
        if (gatt == null) {
            aapsLogger.error(LTag.PUMPBTCOMM, "setNotification_blocking: not configured!")
            retValue.resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
            return retValue
        }
        gattOperationSema.acquire()
        try {
            SystemClock.sleep(1)
            if (mCurrentOperation != null) {
                retValue.resultCode = BLECommOperationResult.RESULT_BUSY
                return retValue
            }
            if (gatt.getService(serviceUUID) == null) {
                retValue.resultCode = BLECommOperationResult.RESULT_NONE
                aapsLogger.error(LTag.PUMPBTCOMM, "BT Device not supported")
                return retValue
            }
            val chara = gatt.getService(serviceUUID)?.getCharacteristic(charaUUID)
            if (chara == null) {
                retValue.resultCode = BLECommOperationResult.RESULT_NONE
                return retValue
            }
            gatt.setCharacteristicNotification(chara, true)
            val list = chara.descriptors
            if (list.isEmpty()) {
                retValue.resultCode = BLECommOperationResult.RESULT_NONE
                return retValue
            }
            if (gattDebugEnabled) {
                for (i in list.indices) {
                    aapsLogger.debug(LTag.PUMPBTCOMM, "Found descriptor: ${list[i]}")
                }
            }
            mCurrentOperation = DescriptorWriteOperation(aapsLogger, gatt, list[0], BluetoothGattDescriptor.ENABLE_NOTIFICATION_VALUE)
            mCurrentOperation?.execute(this)
            when {
                mCurrentOperation?.timedOut == true    -> retValue.resultCode = BLECommOperationResult.RESULT_TIMEOUT
                mCurrentOperation?.interrupted == true -> retValue.resultCode = BLECommOperationResult.RESULT_INTERRUPTED
                else                                   -> retValue.resultCode = BLECommOperationResult.RESULT_SUCCESS
            }
        } finally {
            mCurrentOperation = null
            gattOperationSema.release()
        }
        return retValue
    }

    fun writeCharacteristicBlocking(serviceUUID: UUID, charaUUID: UUID, value: ByteArray): BLECommOperationResult {
        val retValue = BLECommOperationResult()
        val gatt = gattLock.withLock { bluetoothConnectionGatt }
        if (gatt == null) {
            aapsLogger.error(LTag.PUMPBTCOMM, "writeCharacteristic_blocking: not configured!")
            retValue.resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
            return retValue
        }
        retValue.value = value
        gattOperationSema.acquire()
        try {
            SystemClock.sleep(1)
            if (mCurrentOperation != null) {
                retValue.resultCode = BLECommOperationResult.RESULT_BUSY
                return retValue
            }
            if (gatt.getService(serviceUUID) == null) {
                retValue.resultCode = BLECommOperationResult.RESULT_NONE
                aapsLogger.error(LTag.PUMPBTCOMM, "BT Device not supported")
                return retValue
            }
            val chara = gatt.getService(serviceUUID)?.getCharacteristic(charaUUID)
            if (chara == null) {
                retValue.resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
                return retValue
            }
            mCurrentOperation = CharacteristicWriteOperation(aapsLogger, gatt, chara, value)
            mCurrentOperation?.execute(this)
            when {
                mCurrentOperation?.timedOut == true    -> retValue.resultCode = BLECommOperationResult.RESULT_TIMEOUT
                mCurrentOperation?.interrupted == true -> retValue.resultCode = BLECommOperationResult.RESULT_INTERRUPTED
                else                                   -> retValue.resultCode = BLECommOperationResult.RESULT_SUCCESS
            }
        } finally {
            mCurrentOperation = null
            gattOperationSema.release()
        }
        return retValue
    }

    fun readCharacteristicBlocking(serviceUUID: UUID?, charaUUID: UUID?): BLECommOperationResult {
        val retValue = BLECommOperationResult()
        val gatt = gattLock.withLock { bluetoothConnectionGatt }
        if (gatt == null) {
            aapsLogger.error(LTag.PUMPBTCOMM, "readCharacteristic_blocking: not configured!")
            retValue.resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
            return retValue
        }

        gattOperationSema.acquire()
        try {
            SystemClock.sleep(1)
            if (mCurrentOperation != null) {
                retValue.resultCode = BLECommOperationResult.RESULT_BUSY
                return retValue
            }
            if (gatt.getService(serviceUUID) == null) {
                retValue.resultCode = BLECommOperationResult.RESULT_NONE
                aapsLogger.error(LTag.PUMPBTCOMM, "BT Device not supported")
                return retValue
            }
            val chara = gatt.getService(serviceUUID)?.getCharacteristic(charaUUID)
            if (chara == null) {
                retValue.resultCode = BLECommOperationResult.RESULT_NOT_CONFIGURED
                return retValue
            }
            mCurrentOperation = CharacteristicReadOperation(aapsLogger, gatt, chara)
            mCurrentOperation?.execute(this)
            when {
                mCurrentOperation?.timedOut == true    -> retValue.resultCode = BLECommOperationResult.RESULT_TIMEOUT
                mCurrentOperation?.interrupted == true -> retValue.resultCode = BLECommOperationResult.RESULT_INTERRUPTED
                else                                   -> {
                    retValue.resultCode = BLECommOperationResult.RESULT_SUCCESS
                    retValue.value = mCurrentOperation?.value
                }
            }
        } finally {
            mCurrentOperation = null
            gattOperationSema.release()
        }

        return retValue
    }

    private fun getGattStatusMessage(status: Int): String =
        when (status) {
            BluetoothGatt.GATT_SUCCESS             -> "SUCCESS"
            BluetoothGatt.GATT_FAILURE             -> "FAILED"
            BluetoothGatt.GATT_WRITE_NOT_PERMITTED -> "NOT PERMITTED"
            GATT_ERROR_133                         -> "GATT_ERROR_133 (connection issue)"
            else                                   -> "UNKNOWN ($status)"
        }

    private fun getStateMessage(state: Int): String = when (state) {
        BluetoothProfile.STATE_CONNECTED     -> "CONNECTED"
        BluetoothProfile.STATE_CONNECTING    -> "CONNECTING"
        BluetoothProfile.STATE_DISCONNECTED  -> "DISCONNECTED"
        BluetoothProfile.STATE_DISCONNECTING -> "DISCONNECTING"
        else                                 -> "UNKNOWN ($state)"
    }

    init {
        bluetoothGattCallback = object : BluetoothGattCallback() {

            // ===== Android 13+ (API 33) callback with value parameter =====

            override fun onCharacteristicChanged(
                gatt: BluetoothGatt,
                characteristic: BluetoothGattCharacteristic,
                value: ByteArray
            ) {
                handleCharacteristicChanged(characteristic, value)
            }

            @Suppress("DEPRECATION", "OVERRIDE_DEPRECATION")
            override fun onCharacteristicChanged(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic) {
                super.onCharacteristicChanged(gatt, characteristic)
                // Legacy callback for Android < 13
                handleCharacteristicChanged(characteristic, characteristic.value ?: ByteArray(0))
            }

            private fun handleCharacteristicChanged(characteristic: BluetoothGattCharacteristic, value: ByteArray) {
                if (gattDebugEnabled) {
                    aapsLogger.debug(LTag.PUMPBTCOMM,
                        "${ThreadUtil.sig()}onCharacteristicChanged ${GattAttributes.lookup(characteristic.uuid)} ${ByteUtil.getHex(value)}")
                }
                if (characteristic.uuid == UUID.fromString(GattAttributes.CHARA_RADIO_RESPONSE_COUNT)) {
                    if (gattDebugEnabled) {
                        aapsLogger.debug(LTag.PUMPBTCOMM, "Response Count: ${ByteUtil.shortHexString(value)}")
                    }
                    radioResponseCountNotified?.run()
                }
                orangeLink.onCharacteristicChanged(characteristic, value)
            }

            // ===== Android 13+ (API 33) callback with value parameter =====

            override fun onCharacteristicRead(
                gatt: BluetoothGatt,
                characteristic: BluetoothGattCharacteristic,
                value: ByteArray,
                status: Int
            ) {
                handleCharacteristicRead(characteristic, value, status)
            }

            @Suppress("OVERRIDE_DEPRECATION", "DEPRECATION")
            override fun onCharacteristicRead(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
                super.onCharacteristicRead(gatt, characteristic, status)
                // Legacy callback for Android < 13
                handleCharacteristicRead(characteristic, characteristic.value ?: ByteArray(0), status)
            }

            private fun handleCharacteristicRead(characteristic: BluetoothGattCharacteristic, value: ByteArray, status: Int) {
                if (gattDebugEnabled) {
                    aapsLogger.debug(LTag.PUMPBTCOMM,
                        "${ThreadUtil.sig()}onCharacteristicRead (${GattAttributes.lookup(characteristic.uuid)}) " +
                        "${getGattStatusMessage(status)}: ${ByteUtil.getHex(value)}")
                }
                mCurrentOperation?.gattOperationCompletionCallback(characteristic.uuid, value)
            }

            @Suppress("DEPRECATION")
            override fun onCharacteristicWrite(gatt: BluetoothGatt, characteristic: BluetoothGattCharacteristic, status: Int) {
                super.onCharacteristicWrite(gatt, characteristic, status)
                if (gattDebugEnabled) {
                    aapsLogger.debug(LTag.PUMPBTCOMM,
                        "${ThreadUtil.sig()}onCharacteristicWrite ${getGattStatusMessage(status)} " +
                        "${GattAttributes.lookup(characteristic.uuid)} ${ByteUtil.shortHexString(characteristic.value)}")
                }
                mCurrentOperation?.gattOperationCompletionCallback(characteristic.uuid, characteristic.value ?: ByteArray(0))
            }

            override fun onConnectionStateChange(gatt: BluetoothGatt, status: Int, newState: Int) {
                super.onConnectionStateChange(gatt, status, newState)

                // Handle GATT 133 error with retry
                if (status == GATT_ERROR_133) {
                    handleGatt133Error()
                    return
                }

                if (gattDebugEnabled) {
                    aapsLogger.warn(LTag.PUMPBTCOMM,
                        "onConnectionStateChange ${getGattStatusMessage(status)} ${getStateMessage(newState)}")
                }

                when (newState) {
                    BluetoothProfile.STATE_CONNECTED -> {
                        cancelAutoConnectCheck()
                        stopBackgroundReconnect()  // Stop background reconnect on successful connection
                        gatt133RetryCount.set(0)  // Reset on successful connection

                        if (status == BluetoothGatt.GATT_SUCCESS) {
                            // Request connection optimizations before service discovery
                            requestHighPriority()
                            requestMtu()
                            rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.BluetoothConnected)
                        } else {
                            aapsLogger.debug(LTag.PUMPBTCOMM,
                                "BT connected but GATT status: $status (${getGattStatusMessage(status)})")
                        }
                    }

                    BluetoothProfile.STATE_DISCONNECTED -> {
                        rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkDisconnected)
                        isConnected = false
                        lastDisconnectTime = SystemClock.elapsedRealtime()

                        // Reset semaphore atomically
                        gattLock.withLock {
                            mCurrentOperation = null
                            gattOperationSema.drainPermits()
                            gattOperationSema.release()
                        }

                        if (manualDisconnect) {
                            close()
                        } else {
                            // Schedule autoConnect fallback check
                            scheduleAutoConnectCheck()
                        }

                        aapsLogger.warn(LTag.PUMPBTCOMM, "RileyLink Disconnected")
                    }

                    BluetoothProfile.STATE_CONNECTING, BluetoothProfile.STATE_DISCONNECTING -> {
                        aapsLogger.debug(LTag.PUMPBTCOMM, "State: ${getStateMessage(newState)}")
                    }

                    else -> {
                        aapsLogger.warn(LTag.PUMPBTCOMM,
                            String.format(Locale.ENGLISH, "Unknown state: (status=%d, newState=%d)", status, newState))
                    }
                }
            }

            @Suppress("DEPRECATION")
            override fun onDescriptorWrite(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                super.onDescriptorWrite(gatt, descriptor, status)
                if (gattDebugEnabled) {
                    aapsLogger.warn(LTag.PUMPBTCOMM,
                        "onDescriptorWrite ${GattAttributes.lookup(descriptor.uuid)} " +
                        "${getGattStatusMessage(status)} written: ${ByteUtil.getHex(descriptor.value)}")
                }
                mCurrentOperation?.gattOperationCompletionCallback(descriptor.uuid, descriptor.value ?: ByteArray(0))
            }

            @Suppress("DEPRECATION")
            override fun onDescriptorRead(gatt: BluetoothGatt, descriptor: BluetoothGattDescriptor, status: Int) {
                super.onDescriptorRead(gatt, descriptor, status)
                if (gattDebugEnabled) {
                    aapsLogger.warn(LTag.PUMPBTCOMM,
                        "onDescriptorRead ${getGattStatusMessage(status)} ${GattAttributes.lookup(descriptor.uuid)}: ${ByteUtil.getHex(descriptor.value)}")
                }
                mCurrentOperation?.gattOperationCompletionCallback(descriptor.uuid, descriptor.value ?: ByteArray(0))
            }

            override fun onMtuChanged(gatt: BluetoothGatt, mtu: Int, status: Int) {
                super.onMtuChanged(gatt, mtu, status)
                if (gattDebugEnabled) {
                    aapsLogger.warn(LTag.PUMPBTCOMM, "onMtuChanged $mtu status $status")
                }
            }

            override fun onReadRemoteRssi(gatt: BluetoothGatt, rssi: Int, status: Int) {
                super.onReadRemoteRssi(gatt, rssi, status)
                if (gattDebugEnabled) {
                    aapsLogger.warn(LTag.PUMPBTCOMM, "onReadRemoteRssi ${getGattStatusMessage(status)}: $rssi")
                }
            }

            override fun onReliableWriteCompleted(gatt: BluetoothGatt, status: Int) {
                super.onReliableWriteCompleted(gatt, status)
                if (gattDebugEnabled) {
                    aapsLogger.warn(LTag.PUMPBTCOMM, "onReliableWriteCompleted status $status")
                }
            }

            override fun onServicesDiscovered(gatt: BluetoothGatt, status: Int) {
                super.onServicesDiscovered(gatt, status)
                // Cancel timeout - we got a response
                cancelServiceDiscoveryTimeout()

                if (status == BluetoothGatt.GATT_SUCCESS) {
                    val services = gatt.services
                    var rileyLinkFound = false
                    orangeLink.resetOrangeLinkData()
                    val stringBuilder = StringBuilder("RileyLink Device Debug\n")
                    for (service in services) {
                        val uuidService = service.uuid
                        if (isAnyRileyLinkServiceFound(service)) {
                            rileyLinkFound = true
                        }
                        if (gattDebugEnabled) {
                            debugService(service, 0, stringBuilder)
                        }
                        orangeLink.checkIsOrange(uuidService)
                    }
                    if (gattDebugEnabled) {
                        aapsLogger.warn(LTag.PUMPBTCOMM, stringBuilder.toString())
                        aapsLogger.warn(LTag.PUMPBTCOMM, "onServicesDiscovered ${getGattStatusMessage(status)}")
                    }
                    aapsLogger.info(LTag.PUMPBTCOMM, "Gatt device is RileyLink device: $rileyLinkFound")
                    if (rileyLinkFound) {
                        isConnected = true
                        rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkReady)
                    } else {
                        isConnected = false
                        rileyLinkServiceData.setServiceState(
                            RileyLinkServiceState.RileyLinkError,
                            RileyLinkError.DeviceIsNotRileyLink
                        )
                    }
                } else {
                    aapsLogger.debug(LTag.PUMPBTCOMM, "onServicesDiscovered ${getGattStatusMessage(status)}")
                    rileyLinkUtil.sendBroadcastMessage(RileyLinkConst.Intents.RileyLinkGattFailed)
                }
            }
        }
    }
}
