package app.aaps.pump.common.hw.rileylink.service.tasks

import android.os.SystemClock
import app.aaps.core.interfaces.plugin.ActivePlugin
import app.aaps.pump.common.hw.rileylink.service.RileyLinkServiceData
import javax.inject.Inject

class DiscoverGattServicesTask @Inject constructor(
    activePlugin: ActivePlugin,
    private val rileyLinkServiceData: RileyLinkServiceData
) : ServiceTask(activePlugin) {

    companion object {
        // OrangeLink needs delay after MTU negotiation on Android 12+
        private const val ORANGELINK_PRE_DISCOVERY_DELAY_MS = 500L
        // RileyLink/EmaLink - no delay needed (like master branch)
        private const val RILEYLINK_PRE_DISCOVERY_DELAY_MS = 0L
    }

    private var needToConnect: Boolean = false

    fun with(needToConnect: Boolean): DiscoverGattServicesTask {
        this.needToConnect = needToConnect
        return this
    }

    override fun run() {
        if (needToConnect) pumpDevice?.rileyLinkService?.rileyLinkBLE?.connectGatt()

        // Delay before service discovery - required on Android 12+ after MTU negotiation
        // Only for OrangeLink; RileyLink/EmaLink don't need this
        val delayMs = if (rileyLinkServiceData.isOrange) ORANGELINK_PRE_DISCOVERY_DELAY_MS
                      else RILEYLINK_PRE_DISCOVERY_DELAY_MS
        if (delayMs > 0) {
            SystemClock.sleep(delayMs)
        }

        pumpDevice?.rileyLinkService?.rileyLinkBLE?.discoverServices()
    }
}