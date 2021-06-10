import GarbageMap from "../common/GarbageMap";
import { ChannelName, ErrorMessage, TaskHash, TaskStatus } from "../common/types/kacheryTypes";
import { randomAlphaString } from "../common/util";

type ListenerCallback = (status: TaskStatus, errMsg: ErrorMessage | undefined) => void

interface TaskCode extends String {
    __taskCode__: never // phantom type
}
const createTaskCode = (channelName: ChannelName, taskHash: TaskHash) => {
    return `${channelName}:${taskHash}` as any as TaskCode
}

type OutgoingTask = {
    channelName: ChannelName
    taskHash: TaskHash
    status: TaskStatus
    errorMessage?: ErrorMessage
    listenForStatusUpdates: (callback: () => void) => {cancelListener: () => void}
    _callbacks: {[key: string]: () => void}
}

export default class OutgoingTaskManager {
    #outgoingTasksByCode = new GarbageMap<TaskCode, OutgoingTask>(null)
    constructor() {
    }
    createOutgoingTask(channelName: ChannelName, taskHash: TaskHash) {
        const code = createTaskCode(channelName, taskHash)
        if (!this.#outgoingTasksByCode.has(code)) {
            const _callbacks: {[key: string]: () => void} = {}
            const t: OutgoingTask = {
                channelName,
                taskHash,
                status: 'waiting',
                listenForStatusUpdates: (callback: () => void) => {
                    const key = randomAlphaString(10)
                    _callbacks[key] = callback
                    return {cancelListener: () => {
                        if (_callbacks[key]) delete _callbacks[key]
                    }}
                },
                _callbacks
            }
            this.#outgoingTasksByCode.set(code, t)
        }
        const t = this.outgoingTask(channelName, taskHash)
        if (!t) throw Error('Unexpected')
        return t
    }
    outgoingTask(channelName: ChannelName, taskHash: TaskHash) {
        const code = createTaskCode(channelName, taskHash)
        return this.#outgoingTasksByCode.get(code)
    }
    updateTaskStatus(channelName: ChannelName, taskHash: TaskHash, status: TaskStatus, errMsg: ErrorMessage | undefined) {
        const code = createTaskCode(channelName, taskHash)
        const a = this.#outgoingTasksByCode.get(code)
        if (!a) return
        if (a.status !== status) {
            a.status = status
            a.errorMessage = errMsg
            for (let k in a._callbacks) {
                a._callbacks[k]()
            }
        }
    }
}