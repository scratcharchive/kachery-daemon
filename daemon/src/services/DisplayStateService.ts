import { byteCount, durationGreaterThan, elapsedSince, nowTimestamp, Port, unscaledDurationMsec } from "../common/types/kacheryTypes";
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
    _updateDisplay() {
        const lines: string[] = []
        lines.push('')
        lines.push('=======================================')
        lines.push(`NODE ${this.#node.nodeId().slice(0, 6)} (${this.#node.nodeLabel()})`)
        if (this.opts.daemonApiPort)
            lines.push(`http://localhost:${this.opts.daemonApiPort}/stats?format=html`)
        lines.push('=======================================')
        const txt = lines.join('\n')
        const elapsed = unscaledDurationMsec(elapsedSince(this.#lastDisplayTimestamp))
        if ((txt !== this.#lastText) || (durationGreaterThan(elapsed, unscaledDurationMsec(30000)))) {
            this.#lastText = txt
            this.#lastDisplayTimestamp = nowTimestamp()
            console.info(txt)
            console.info(`Downloaded: ${formatByteCount(this.#node.getStats({format: 'json'}).totalBytesReceived.total)}; Uploaded: ${formatByteCount(this.#node.getStats({format: 'json'}).totalBytesSent.total)};`)
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