import GarbageMap from "../common/GarbageMap";
import { ChannelName, DurationMsec, durationMsecToNumber, elapsedSince, nowTimestamp, scaledDurationMsec, Sha1Hash, TaskFunctionId, TaskKwargs, Timestamp } from "../common/types/kacheryTypes";
import { randomAlphaString } from "../common/util";
import KacheryHubInterface from "../kacheryHub/KacheryHubInterface";
import { RegisteredTaskFunction, RequestedTask, TaskStatus } from "../services/daemonApiTypes";

type RegisteredTaskFunctionGroup = {
    taskFunctions: RegisteredTaskFunction[]
    incomingRequestedTasksCallback?: (incomingRequestedTasks: RequestedTask[]) => void
    internalRequestedTaskList: RequestedTask[]
}

type PendingTaskRequest = {
    requestedTask: RequestedTask
    timestamp: Timestamp
}

export default class IncomingTaskManager {
    #registeredTaskFunctionGroups = new GarbageMap<string, RegisteredTaskFunctionGroup>(null)
    #pendingTaskRequests: PendingTaskRequest[] = []
    #processScheduled = false
    constructor() {
    }
    async registerTaskFunctions(args: {taskFunctions: RegisteredTaskFunction[], timeoutMsec: DurationMsec}): Promise<RequestedTask[]> {
        const {taskFunctions, timeoutMsec} = args
        return new Promise<RequestedTask[]>((resolve, reject) => {
            let complete = false
            const key = randomAlphaString(10)
            const _return = (retval: RequestedTask[]) => {
                if (complete) return
                complete = true
                this.#registeredTaskFunctionGroups.delete(key)
                resolve(retval)
            }
            const tfg: RegisteredTaskFunctionGroup = {
                taskFunctions,
                incomingRequestedTasksCallback: (incomingRequestedTasks) => {
                    if (complete) throw Error('Unexpected complete')
                    _return(incomingRequestedTasks)
                },
                internalRequestedTaskList: []
            }
            this.#registeredTaskFunctionGroups.set(key, tfg)
            this._processPendingTaskRequests()
            setTimeout(() => {
                if (!complete) {
                    _return([])
                }
            }, durationMsecToNumber(timeoutMsec))
        })
    }
    requestTaskResult(channelName: ChannelName, taskHash: Sha1Hash, taskFunctionId: TaskFunctionId, taskKwargs: TaskKwargs) {
        this.#pendingTaskRequests.push({
            requestedTask: {
                channelName,
                taskHash,
                taskFunctionId,
                kwargs: taskKwargs
            },
            timestamp: nowTimestamp()
        })
        this._scheduleProcessPendingTaskRequests()
    }
    _scheduleProcessPendingTaskRequests() {
        if (this.#processScheduled) return
        this.#processScheduled = true
        setTimeout(() => {
            this.#processScheduled = false
            this._processPendingTaskRequests()
        }, 100)
    }
    _processPendingTaskRequests() {
        const newList: PendingTaskRequest[] = []
        for (let x of this.#pendingTaskRequests) {
            let remove = false
            const elapsed = elapsedSince(x.timestamp)
            if (elapsed < 1000 * 4) {
                const g = this._findRegisteredTaskFunctionGroupForTaskFunction(x.requestedTask.taskFunctionId)
                if (g) {
                    g.internalRequestedTaskList.push(x.requestedTask)
                    remove = true
                }
            }
            else remove = true
            if (!remove) newList.push(x)
        }
        this.#pendingTaskRequests = newList

        for (let k of this.#registeredTaskFunctionGroups.keys()) {
            const g = this.#registeredTaskFunctionGroups.get(k)
            if (!g) throw Error('Unexpected')
            if (g.internalRequestedTaskList.length > 0) {
                g.incomingRequestedTasksCallback && g.incomingRequestedTasksCallback(g.internalRequestedTaskList)
                g.internalRequestedTaskList = []
            }
        }
    }
    _findRegisteredTaskFunctionGroupForTaskFunction(taskFunctionId: TaskFunctionId) {
        for (let k of this.#registeredTaskFunctionGroups.keys()) {
            const g = this.#registeredTaskFunctionGroups.get(k)
            if (!g) throw Error('Unexpected')
            for (let f of g.taskFunctions) {
                if (f.taskFunctionId === taskFunctionId) {
                    return g
                }
            }
        }
        return null
    }
}