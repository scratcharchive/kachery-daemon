import GarbageMap from "../common/GarbageMap";
import { channelName, ChannelName, ErrorMessage, Sha1Hash } from "../common/types/kacheryTypes";
import { randomAlphaString } from "../common/util";
import { TaskStatus } from "../services/daemonApiTypes";

type ListenerCallback = (status: TaskStatus, errMsg: ErrorMessage | undefined) => void

interface TaskCode extends String {
    __taskCode__: never // phantom type
}
const createTaskCode = (channelName: ChannelName, taskHash: Sha1Hash) => {
    return `${channelName}:${taskHash}` as any as TaskCode
}

export default class OutgoingTaskManager {
    #taskStatusUpdateListenersByTaskHash = new GarbageMap<TaskCode, {[key: string]: ListenerCallback}>(null)
    constructor() {
    }
    updateTaskStatus(channelName: ChannelName, taskHash: Sha1Hash, status: TaskStatus, errMsg: ErrorMessage | undefined) {
        const code = createTaskCode(channelName, taskHash)
        const a = this.#taskStatusUpdateListenersByTaskHash.get(code)
        if (!a) return
        for (let k in a) {
            a[k](status, errMsg)
        }
    }
    listenForTaskStatusUpdates(channelName: ChannelName, taskHash: Sha1Hash, callback: ListenerCallback) {
        const code = createTaskCode(channelName, taskHash)
        const key = randomAlphaString(10)
        if (!this.#taskStatusUpdateListenersByTaskHash.has(code)) {
            this.#taskStatusUpdateListenersByTaskHash.set(code, {})
        }
        const a = this.#taskStatusUpdateListenersByTaskHash.get(code)
        if (!a) throw Error('Unexpected')
        a[key] = callback
        const cancelListener = () => {
            if (a[key]) delete a[key]
        }
        return {cancelListener}
    }
}