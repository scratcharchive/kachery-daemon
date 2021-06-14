import { NodeChannelMembership } from "../kachery-js/types/kacheryHubTypes";
import { byteCount, durationGreaterThan, elapsedSince, nowTimestamp, Port, unscaledDurationMsec } from "../kachery-js/types/kacheryTypes";
import { formatByteCount, sleepMsec } from "../common/util";
import KacheryDaemonNode from "../KacheryDaemonNode";

export default class DisplayStateService {
    #node: KacheryDaemonNode
    #halted = false
    #lastText = ''
    #lastDisplayTimestamp = nowTimestamp()
    #intervalMsec = unscaledDurationMsec(10000)
    constructor(node: KacheryDaemonNode, private opts: {daemonApiPort: Port | null}) {
        this.#node = node

        this._start()
    }
    stop() {
        this.#halted = true
    }
    async _updateDisplay() {
        const lines: string[] = []
        lines.push('')
        lines.push('=======================================')
        lines.push(`NODE ${this.#node.nodeId()} (${this.#node.nodeLabel()})`)
        const nodeConfig = await this.#node.kacheryHubInterface().getNodeConfig()
        if (nodeConfig) {
            ;(nodeConfig.channelMemberships || []).map(cm => {
                lines.push(`CHANNEL ${cm.channelName} ${makeRoleString(cm)}`)
            })
        }
        if (this.opts.daemonApiPort)
            lines.push(`http://localhost:${this.opts.daemonApiPort}/stats?format=html`)
        lines.push('=======================================')
        const txt = lines.join('\n')
        const elapsed = unscaledDurationMsec(elapsedSince(this.#lastDisplayTimestamp))
        if ((txt !== this.#lastText) || (durationGreaterThan(elapsed, unscaledDurationMsec(30000)))) {
            this.#lastText = txt
            this.#lastDisplayTimestamp = nowTimestamp()
            console.info(txt)
            const stats = this.#node.getStats({format: 'json'})
            console.info(`Downloaded: ${formatByteCount(stats.totalBytesReceived)}; Uploaded: ${formatByteCount(stats.totalBytesSent)}; Messages sent: ${stats.totalMessagesSent}`)
            console.info(`Memory used: ${formatByteCount(byteCount(process.memoryUsage().heapUsed))} (heap); ${formatByteCount(byteCount(process.memoryUsage().external))} (external); ${formatByteCount(byteCount(process.memoryUsage().arrayBuffers))} (arrayBuffers);`)
        }
    }
    async _start() {
        while (true) {
            if (this.#halted) return
            this._updateDisplay()
            await sleepMsec(this.#intervalMsec, () => {return !this.#halted})
        }
    }
}

const makeRoleString = (cm: NodeChannelMembership) => {
    const ret: string[] = []
    if (cm.roles.downloadFiles) ret.push('dfi')
    if (cm.roles.downloadFeeds) ret.push('dfe')
    if (cm.roles.downloadTaskResults) ret.push('dtr')
    if (cm.authorization) {
        if ((cm.roles.requestFiles) && (cm.authorization.permissions.requestFiles))  ret.push('rfi')
        if ((cm.roles.requestFeeds) && (cm.authorization.permissions.requestFeeds)) ret.push('rfe')
        if ((cm.roles.requestTasks) && (cm.authorization.permissions.requestTasks)) ret.push('rtr')
        if ((cm.roles.provideFiles) && (cm.authorization.permissions.provideFiles)) ret.push('pfi')
        if ((cm.roles.provideFeeds) && (cm.authorization.permissions.provideFeeds)) ret.push('pfe')
        if ((cm.roles.provideTasks) && (cm.authorization.permissions.provideTasks)) ret.push('ptr')
    }
    return ret.join(' ')
}