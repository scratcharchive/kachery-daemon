import { isRegisteredTaskFunction, isRequestedTask, RegisteredTaskFunction, RequestedTask } from '../kacheryInterface/kacheryHubTypes';
import { ByteCount, ChannelName, DaemonVersion, DurationMsec, ErrorMessage, FeedId, FeedName, FileKey, isArrayOf, isBoolean, isByteCount, isChannelName, isDaemonVersion, isDurationMsec, isErrorMessage, isFeedId, isFeedName, isFileKey, isJSONValue, isMessageCount, isNodeId, isNull, isNumber, isObjectOf, isOneOf, isSha1Hash, isSignedSubfeedMessage, isString, isSubfeedHash, isSubfeedMessage, isSubfeedWatches, isTaskFunctionId, isTaskFunctionType, isTaskId, isTaskKwargs, isTaskStatus, isUrlString, JSONValue, LocalFilePath, MessageCount, NodeId, optional, Sha1Hash, SignedSubfeedMessage, SubfeedHash, SubfeedMessage, SubfeedWatches, TaskFunctionId, TaskFunctionType, TaskId, TaskKwargs, TaskStatus, UrlString, _validateObject } from '../commonInterface/kacheryTypes';

export interface DaemonApiProbeResponse {
    success: boolean,
    daemonVersion: DaemonVersion,
    nodeId: NodeId,
    kacheryStorageDir: LocalFilePath | null
};
export const isDaemonApiProbeResponse = (x: any): x is DaemonApiProbeResponse => {
    return _validateObject(x, {
        success: isBoolean,
        daemonVersion: isDaemonVersion,
        nodeId: isNodeId,
        kacheryStorageDir: isOneOf([isNull, isString])
    }, {allowAdditionalFields: true});
}

export type StoreFileRequestData = {
    localFilePath: LocalFilePath
}
export const isStoreFileRequestData = (x: any): x is StoreFileRequestData => {
    return _validateObject(x, {
        localFilePath: isString
    })
}
export type StoreFileResponseData = {
    success: boolean
    error: ErrorMessage | null
    sha1: Sha1Hash | null
    manifestSha1: Sha1Hash | null
}

export type LinkFileRequestData = {
    localFilePath: LocalFilePath
    size: number
    mtime: number
}
export const isLinkFileRequestData = (x: any): x is LinkFileRequestData => {
    return _validateObject(x, {
        localFilePath: isString,
        size: isNumber,
        mtime: isNumber
    })
}
export type LinkFileResponseData = {
    success: boolean
    error: ErrorMessage | null
    sha1: Sha1Hash | null
    manifestSha1: Sha1Hash | null
}

export type UploadFileRequestData = {
    sha1: Sha1Hash
    channel: ChannelName
}
export const isUploadFileRequestData = (x: any): x is UploadFileRequestData => {
    return _validateObject(x, {
        sha1: isSha1Hash,
        channel: isChannelName
    })
}
export type UploadFileResponseData = {
    success: boolean
}

// interface Req {
//     body: any,
//     on: (eventName: string, callback: () => void) => void,
//     connection: Socket
// }

// interface Res {
//     json: (obj: {
//         success: boolean
//     } & JSONObject) => void,
//     end: () => void,
//     status: (s: number) => Res,
//     send: (x: any) => Res
// }

export type CreateSignedFileUploadUrlRequest = {
    channelName: ChannelName
    sha1: Sha1Hash
    size?: ByteCount
}

export const isCreateSignedFileUploadUrlRequest = (x: any): x is CreateSignedFileUploadUrlRequest => {
    return _validateObject(x, {
        channelName: isChannelName,
        sha1: isSha1Hash,
        size: optional(isByteCount)
    })
}

export type CreateSignedFileUploadUrlResponse = {
    success: boolean
    alreadyUploaded: boolean
    signedUrl?: UrlString
}

export const isCreateSignedFileUploadUrlResponse = (x: any): x is CreateSignedFileUploadUrlResponse => {
    return _validateObject(x, {
        success: isBoolean,
        alreadyUploaded: isBoolean,
        signedUrl: optional(isUrlString)
    })
}


export interface ApiLoadFileRequest {
    fileKey: FileKey
}
export const isApiLoadFileRequest = (x: any): x is ApiLoadFileRequest => {
    return _validateObject(x, {
        fileKey: isFileKey
    });
}

export interface ApiDownloadFileDataRequest {
    fileKey: FileKey,
    startByte?: ByteCount
    endByte?: ByteCount
}
export const isApiDownloadFileDataRequest = (x: any): x is ApiDownloadFileDataRequest => {
    return _validateObject(x, {
        fileKey: isFileKey,
        startByte: optional(isByteCount),
        endByte: optional(isByteCount),
    });
}

export interface FeedApiWatchForNewMessagesRequest {
    subfeedWatches: SubfeedWatches,
    waitMsec: DurationMsec,
    maxNumMessages?: MessageCount,
    signed?: boolean
}
export const isFeedApiWatchForNewMessagesRequest = (x: any): x is FeedApiWatchForNewMessagesRequest => {
    return _validateObject(x, {
        subfeedWatches: isSubfeedWatches,
        waitMsec: isDurationMsec,
        signed: optional(isBoolean),
        maxNumMessages: optional(isMessageCount)
    })
}
export interface FeedApiWatchForNewMessagesResponse {
    success: boolean,
    messages: {[key: string]: SubfeedMessage[]} | {[key: string]: SignedSubfeedMessage[]}
}
export const isFeedApiWatchForNewMessagesResponse = (x: any): x is FeedApiWatchForNewMessagesResponse => {
    return _validateObject(x, {
        success: isBoolean,
        messages: isOneOf([isObjectOf(isString, isArrayOf(isSubfeedMessage)), isObjectOf(isString, isArrayOf(isSignedSubfeedMessage))])
    })
}

export interface MutableApiSetRequest {
    key: JSONValue
    value: JSONValue
    update?: boolean // default true
}
export const isMutableApiSetRequest = (x: any): x is MutableApiSetRequest => {
    return _validateObject(x, {
        key: isJSONValue,
        value: isJSONValue,
        update: optional(isBoolean) // default true
    })
}
export interface MutableApiSetResponse {
    success: boolean
}
export const isMutableApiSetResponse = (x: any): x is MutableApiSetResponse => {
    return _validateObject(x, {
        success: isBoolean
    })
}

export interface MutableApiGetRequest {
    key: JSONValue
}
export const isMutableApiGetRequest = (x: any): x is MutableApiGetRequest => {
    return _validateObject(x, {
        key: isJSONValue
    })
}
export interface MutableApiGetResponse {
    success: boolean,
    found: boolean,
    value: JSONValue
}
export const isMutableApiGetResponse = (x: any): x is MutableApiGetResponse => {
    return _validateObject(x, {
        success: isBoolean,
        found: isBoolean,
        value: isJSONValue
    })
}

export interface MutableApiDeleteRequest {
    key: JSONValue
}
export const isMutableApiDeleteRequest = (x: any): x is MutableApiDeleteRequest => {
    return _validateObject(x, {
        key: isJSONValue
    })
}
export interface MutableApiDeleteResponse {
    success: boolean
}
export const isMutableApiDeleteResponse = (x: any): x is MutableApiDeleteResponse => {
    return _validateObject(x, {
        success: isBoolean
    })
}

export interface FeedApiCreateFeedRequest {
    feedName?: FeedName
}
export const isFeedApiCreateFeedRequest = (x: any): x is FeedApiCreateFeedRequest => {
    return _validateObject(x, {
        feedName: optional(isFeedName)
    });
}
export interface FeedApiCreateFeedResponse {
    success: boolean,
    feedId: FeedId
}
export const isFeedApiCreateFeedResponse = (x: any): x is FeedApiCreateFeedResponse => {
    return _validateObject(x, {
        success: isBoolean,
        feedId: isFeedId
    });
}

export interface FeedApiAppendMessagesRequest {
    feedId: FeedId,
    subfeedHash: SubfeedHash,
    messages: SubfeedMessage[]
}
export const isFeedApiAppendMessagesRequest = (x: any): x is FeedApiAppendMessagesRequest => {
    return _validateObject(x, {
        feedId: isFeedId,
        subfeedHash: isSubfeedHash,
        messages: isArrayOf(isSubfeedMessage)
    });
}
export interface FeedApiAppendMessagesResponse {
    success: boolean
}
export const isFeedApiAppendMessagesResponse = (x: any): x is FeedApiAppendMessagesResponse => {
    return _validateObject(x, {
        success: isBoolean
    });
}

export interface FeedApiGetNumLocalMessagesRequest {
    feedId: FeedId,
    subfeedHash: SubfeedHash
}
export const isFeedApiGetNumLocalMessagesRequest = (x: any): x is FeedApiGetNumLocalMessagesRequest => {
    return _validateObject(x, {
        feedId: isFeedId,
        subfeedHash: isSubfeedHash
    });
}
export interface FeedApiGetNumLocalMessagesResponse {
    success: boolean,
    numMessages: MessageCount
}
export const isFeedApiGetNumLocalMessagesResponse = (x: any): x is FeedApiGetNumLocalMessagesResponse => {
    return _validateObject(x, {
        success: isBoolean,
        numMessages: isMessageCount
    });
}

export interface FeedApiGetFeedInfoRequest {
    feedId: FeedId
}
export const isFeedApiGetFeedInfoRequest = (x: any): x is FeedApiGetFeedInfoRequest => {
    return _validateObject(x, {
        feedId: isFeedId
    });
}
export interface FeedApiGetFeedInfoResponse {
    success: boolean,
    isWriteable: boolean,
}
export const isFeedApiGetFeedInfoResponse = (x: any): x is FeedApiGetFeedInfoResponse => {
    return _validateObject(x, {
        success: isBoolean,
        isWriteable: isBoolean
    })
}

export interface FeedApiDeleteFeedRequest {
    feedId: FeedId
}
export const isFeedApiDeleteFeedRequest = (x: any): x is FeedApiDeleteFeedRequest => {
    return _validateObject(x, {
        feedId: isFeedId
    });
}
export interface FeedApiDeleteFeedResponse {
    success: boolean
}
export const isFeedApiDeleteFeedResponse = (x: any): x is FeedApiDeleteFeedResponse => {
    return _validateObject(x, {
        success: isBoolean
    });
}

export interface FeedApiGetFeedIdRequest {
    feedName: FeedName
}
export const isFeedApiGetFeedIdRequest = (x: any): x is FeedApiGetFeedIdRequest => {
    return _validateObject(x, {
        feedName: isFeedName
    });
}
export interface FeedApiGetFeedIdResponse {
    success: boolean,
    feedId: FeedId | null
}
export const isFeedApiGetFeedIdResponse = (x: any): x is FeedApiGetFeedIdResponse => {
    return _validateObject(x, {
        success: isBoolean,
        feedId: isOneOf([isNull, isFeedId])
    });
}

export interface TaskRegisterTaskFunctionsRequest {
    taskFunctions: RegisteredTaskFunction[]
    backendId?: string | null
    timeoutMsec: DurationMsec
}
export const isTaskRegisterTaskFunctionsRequest = (x: any): x is TaskRegisterTaskFunctionsRequest => {
    return _validateObject(x, {
        taskFunctions: isArrayOf(isRegisteredTaskFunction),
        backendId: optional(isOneOf([isString, isNull])),
        timeoutMsec: isDurationMsec
    })
}

export interface TaskRegisterTaskFunctionsResponse {
    requestedTasks: RequestedTask[]
    success: boolean
}
export const isTaskRegisterTaskFunctionsResponse = (x: any): x is TaskRegisterTaskFunctionsResponse => {
    return _validateObject(x, {
        tasks: isArrayOf(isRequestedTask),
        success: isBoolean
    });
}

export interface TaskUpdateTaskStatusRequest {
    channelName: ChannelName
    taskId: TaskId,
    status: TaskStatus
    errorMessage?: ErrorMessage
}
export const isTaskUpdateTaskStatusRequest = (x: any): x is TaskUpdateTaskStatusRequest => {
    return _validateObject(x, {
        channelName: isChannelName,
        taskId: isTaskId,
        status: isTaskStatus,
        errorMessage: optional(isErrorMessage)
    })
}

export interface TaskUpdateTaskStatusResponse {
    success: boolean
}
export const isTaskUpdateTaskStatusResponse = (x: any): x is TaskUpdateTaskStatusResponse => {
    return _validateObject(x, {
        success: isBoolean
    });
}

export type TaskCreateSignedTaskResultUploadUrlRequest = {
    channelName: ChannelName
    taskId: TaskId
    size: ByteCount
}

export const isTaskCreateSignedTaskResultUploadUrlRequest = (x: any): x is TaskCreateSignedTaskResultUploadUrlRequest => {
    return _validateObject(x, {
        channelName: isChannelName,
        taskId: isSha1Hash,
        size: isByteCount
    })
}

export type TaskCreateSignedTaskResultUploadUrlResponse = {
    success: boolean
    signedUrl: UrlString
}

export const isTaskCreateSignedTaskResultUploadUrlResponse = (x: any): x is TaskCreateSignedTaskResultUploadUrlResponse => {
    return _validateObject(x, {
        success: isBoolean,
        signedUrl: isUrlString
    })
}

export type TaskRequestTaskRequest = {
    channelName: ChannelName
    backendId?: string | null
    taskFunctionId: TaskFunctionId
    taskKwargs: TaskKwargs
    taskFunctionType: TaskFunctionType
    timeoutMsec: DurationMsec
}

export const isTaskRequestTaskRequest = (x: any): x is TaskRequestTaskRequest => {
    return _validateObject(x, {
        channelName: isChannelName,
        backendId: optional(isOneOf([isString, isNull])),
        taskFunctionId: isTaskFunctionId,
        taskKwargs: isTaskKwargs,
        taskFunctionType: isTaskFunctionType,
        timeoutMsec: isDurationMsec
    })
}

export type TaskRequestTaskResponse = {
    success: boolean
    taskId: TaskId,
    status: TaskStatus
    errorMessage?: ErrorMessage
    taskResultUrl?: UrlString
}

export const isTaskRequestTaskResponse = (x: any): x is TaskRequestTaskResponse => {
    return _validateObject(x, {
        success: isBoolean,
        taskId: isTaskId,
        status: isTaskStatus,
        errorMessage: optional(isErrorMessage),
        taskResultUrl: optional(isUrlString)
    })
}


export type TaskWaitForTaskResultRequest = {
    channelName: ChannelName
    taskId: TaskId
    taskResultUrl: UrlString
    taskFunctionType: TaskFunctionType,
    timeoutMsec: DurationMsec
}

export const isTaskWaitForTaskResultRequest = (x: any): x is TaskWaitForTaskResultRequest => {
    return _validateObject(x, {
        channelName: isChannelName,
        taskId: isTaskId,
        taskResultUrl: isUrlString,
        taskFunctionType: isTaskFunctionType,
        timeoutMsec: isDurationMsec
    })
}

export type TaskWaitForTaskResultResponse = {
    success: boolean
    status: TaskStatus
    errorMessage?: ErrorMessage
}

export const isTaskWaitForTaskResultResponse = (x: any): x is TaskWaitForTaskResultResponse => {
    return _validateObject(x, {
        success: isBoolean,
        status: isTaskStatus,
        errorMessage: optional(isErrorMessage)
    })
}