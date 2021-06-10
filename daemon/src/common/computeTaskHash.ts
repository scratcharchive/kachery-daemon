import { JSONObject, sha1OfObject, TaskFunctionId, TaskKwargs } from "./types/kacheryTypes";

const computeTaskHash = (taskFunctionId: TaskFunctionId, kwargs: TaskKwargs) => {
    const taskData = {
        functionId: taskFunctionId,
        kwargs
    } as any as JSONObject
    const taskHash = sha1OfObject(taskData)
    return taskHash
}

export default computeTaskHash