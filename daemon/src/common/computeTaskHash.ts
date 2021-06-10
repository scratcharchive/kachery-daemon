import { isTaskHash, JSONObject, sha1OfObject, TaskFunctionId, TaskHash, TaskKwargs } from "./types/kacheryTypes";

const computeTaskHash = (taskFunctionId: TaskFunctionId, kwargs: TaskKwargs) => {
    const taskData = {
        functionId: taskFunctionId,
        kwargs
    } as any as JSONObject
    const taskHash = sha1OfObject(taskData)
    if (!isTaskHash(taskHash)) throw Error('Unexpected in computeTaskHash')
    return taskHash as TaskHash
}

export default computeTaskHash