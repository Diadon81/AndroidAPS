package app.aaps.pump.common.hw.rileylink.ble

import android.os.SystemClock
import app.aaps.core.interfaces.logging.AAPSLogger
import app.aaps.core.interfaces.logging.LTag
import app.aaps.core.utils.pump.ByteUtil
import app.aaps.core.utils.pump.ThreadUtil
import app.aaps.pump.common.hw.rileylink.ble.data.GattAttributes
import app.aaps.pump.common.hw.rileylink.ble.defs.RileyLinkEncodingType
import app.aaps.pump.common.hw.rileylink.ble.operations.BLECommOperationResult
import java.util.UUID
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

/**
 * Created by geoff on 5/26/16.
 */
class RFSpyReader internal constructor(private val aapsLogger: AAPSLogger, private val rileyLinkBle: RileyLinkBLE) {

    private var executor: ExecutorService? = null
    private val waitForRadioData = Semaphore(0, true)
    private val mDataQueue = LinkedBlockingQueue<ByteArray>()
    private val acquireCount = AtomicInteger(0)
    private val releaseCount = AtomicInteger(0)
    @Volatile
    private var stopAtNull = true
    private val running = AtomicBoolean(false)
    private val lifecycleLock = Any()

    fun setRileyLinkEncodingType(encodingType: RileyLinkEncodingType) {
        aapsLogger.debug("setRileyLinkEncodingType: $encodingType")
        stopAtNull = !(encodingType == RileyLinkEncodingType.Manchester || encodingType == RileyLinkEncodingType.FourByteSixByteRileyLink)
    }

    // This timeout must be coordinated with the length of the RFSpy radio operation or Bad Things Happen.
    fun poll(timeoutMs: Int): ByteArray? {
        aapsLogger.debug(LTag.PUMPBTCOMM, "${ThreadUtil.sig()}Entering poll at t==${SystemClock.uptimeMillis()}, timeout is $timeoutMs mDataQueue size is ${mDataQueue.size}")
        if (mDataQueue.isEmpty() || timeoutMs == 0) { //0 timeout is used for drain queue in RFSpy.writeToDataRaw before sending new command
            try {
                // block until timeout or data available.
                // returns null if timeout.
                val dataFromQueue = mDataQueue.poll(timeoutMs.toLong(), TimeUnit.MILLISECONDS)
                if (dataFromQueue != null)
                    aapsLogger.debug(LTag.PUMPBTCOMM, "Got data [${ByteUtil.shortHexString(dataFromQueue)}] at t==${SystemClock.uptimeMillis()}")
                else
                    aapsLogger.debug(LTag.PUMPBTCOMM, "Got data [null] at t==" + SystemClock.uptimeMillis())
                return dataFromQueue
            } catch (e: InterruptedException) {
                aapsLogger.error(LTag.PUMPBTCOMM, "poll: Interrupted waiting for data")
                Thread.currentThread().interrupt() // Restore interrupt status
            }
        }
        return null
    }

    // Call this from the "response count" notification handler.
    fun newDataIsAvailable() {
        val count = releaseCount.incrementAndGet()
        aapsLogger.debug(LTag.PUMPBTCOMM, "${ThreadUtil.sig()}waitForRadioData released(count=$count) at t=${SystemClock.uptimeMillis()}")
        waitForRadioData.release()
    }

    fun start() {
        synchronized(lifecycleLock) {
            if (!running.compareAndSet(false, true)) {
                aapsLogger.debug(LTag.PUMPBTCOMM, "RFSpyReader already running")
                return
            }
            // Reset all state to clean before starting
            waitForRadioData.drainPermits()
            acquireCount.set(0)
            releaseCount.set(0)
            mDataQueue.clear()  // Clear any stale data from previous sessions
            // Create a new executor for this session
            executor = Executors.newSingleThreadExecutor()
            executor?.execute {
                val serviceUUID = UUID.fromString(GattAttributes.SERVICE_RADIO)
                val radioDataUUID = UUID.fromString(GattAttributes.CHARA_RADIO_DATA)
                aapsLogger.debug(LTag.PUMPBTCOMM, "RFSpyReader started")
                while (running.get()) {
                    try {
                        val count = acquireCount.incrementAndGet()
                        waitForRadioData.acquire()
                        if (!running.get()) {
                            aapsLogger.debug(LTag.PUMPBTCOMM, "RFSpyReader stopping after acquire")
                            break
                        }
                        aapsLogger.debug(LTag.PUMPBTCOMM, "${ThreadUtil.sig()}waitForRadioData acquired (count=$count) at t=${SystemClock.uptimeMillis()}")
                        SystemClock.sleep(1)
                        val result = rileyLinkBle.readCharacteristicBlocking(serviceUUID, radioDataUUID)
                        SystemClock.sleep(1)
                        if (result.resultCode == BLECommOperationResult.RESULT_SUCCESS) {
                            if (stopAtNull) {
                                // only data up to the first null is valid
                                result.value?.let { resultValue ->
                                    for (i in resultValue.indices) {
                                        if (resultValue[i].toInt() == 0) {
                                            result.value = ByteUtil.substring(resultValue, 0, i)
                                            break
                                        }
                                    }
                                }
                            }
                            mDataQueue.add(result.value)
                        } else if (result.resultCode == BLECommOperationResult.RESULT_INTERRUPTED)
                            aapsLogger.error(LTag.PUMPBTCOMM, "Read operation was interrupted")
                        else if (result.resultCode == BLECommOperationResult.RESULT_TIMEOUT)
                            aapsLogger.error(LTag.PUMPBTCOMM, "Read operation on Radio Data timed out")
                        else if (result.resultCode == BLECommOperationResult.RESULT_BUSY)
                            aapsLogger.error(LTag.PUMPBTCOMM, "FAIL: RileyLinkBLE reports operation already in progress")
                        else if (result.resultCode == BLECommOperationResult.RESULT_NONE)
                            aapsLogger.error(LTag.PUMPBTCOMM, "FAIL: got invalid result code: ${result.resultCode}")
                    } catch (e: InterruptedException) {
                        aapsLogger.error(LTag.PUMPBTCOMM, "Interrupted while waiting for data")
                        Thread.currentThread().interrupt() // Restore interrupt status
                        break // Exit loop on interrupt
                    }
                }
                aapsLogger.debug(LTag.PUMPBTCOMM, "RFSpyReader stopped")
            }
        }
    }

    fun stop() {
        synchronized(lifecycleLock) {
            aapsLogger.debug(LTag.PUMPBTCOMM, "Stopping RFSpyReader")
            running.set(false)
            waitForRadioData.release() // Unblock acquire() if waiting
            mDataQueue.clear()
            // Shutdown executor and wait for termination
            executor?.let { exec ->
                exec.shutdown()
                try {
                    if (!exec.awaitTermination(5, TimeUnit.SECONDS)) {
                        aapsLogger.warn(LTag.PUMPBTCOMM, "RFSpyReader executor did not terminate in time, forcing shutdown")
                        exec.shutdownNow()
                    }
                } catch (e: InterruptedException) {
                    aapsLogger.error(LTag.PUMPBTCOMM, "Interrupted while waiting for executor shutdown")
                    exec.shutdownNow()
                    Thread.currentThread().interrupt()
                }
            }
            executor = null
        }
    }
}
