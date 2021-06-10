import { isNodeConfig, NodeConfig } from "./kacheryHubTypes"
import { ByteCount, ChannelName, FeedId, isArrayOf, isBoolean, isByteCount, isChannelName, isEqualTo, isFeedId, isNodeId, isNodeLabel, isNumber, isOneOf, isSha1Hash, isSignature, isString, isSubfeedHash, isTaskHash, isUrlString, isUserId, NodeId, NodeLabel, optional, Sha1Hash, Signature, SubfeedHash, TaskHash, UrlString, UserId, _validateObject } from "./kacheryTypes"

export type ReportRequestBody = {
    type: 'report'
    nodeId: NodeId
    ownerId: UserId
    nodeLabel: NodeLabel
}

export const isReportRequestBody = (x: any): x is ReportRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('report'),
        nodeId: isNodeId,
        ownerId: isUserId,
        nodeLabel: isNodeLabel
    })
}

export type GetNodeConfigRequestBody = {
    type: 'getNodeConfig'
    nodeId: NodeId
    ownerId: UserId
}

export const isGetNodeConfigRequestBody = (x: any): x is GetNodeConfigRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('getNodeConfig'),
        nodeId: isNodeId,
        ownerId: isUserId
    })
}

export type GetNodeConfigResponse = {
    found: boolean,
    nodeConfig?: NodeConfig
}

export const isGetNodeConfigResponse = (x: any): x is GetNodeConfigResponse => {
    return _validateObject(x, {
        found: isBoolean,
        nodeConfig: optional(isNodeConfig)
    })
}

export type GetPubsubAuthForChannelRequestBody = {
    type: 'getPubsubAuthForChannel'
    nodeId: NodeId
    ownerId: UserId,
    channelName: ChannelName
}

export const isGetPubsubAuthForChannelRequestBody = (x: any): x is GetPubsubAuthForChannelRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('getPubsubAuthForChannel'),
        nodeId: isNodeId,
        ownerId: isUserId,
        channelName: isChannelName
    })
}

export type CreateSignedFileUploadUrlRequestBody = {
    type: 'createSignedFileUploadUrl'
    nodeId: NodeId
    ownerId: UserId
    channelName: ChannelName
    sha1: Sha1Hash
    size: ByteCount
}

export const isCreateSignedFileUploadUrlRequestBody = (x: any): x is CreateSignedFileUploadUrlRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('createSignedFileUploadUrl'),
        nodeId: isNodeId,
        ownerId: isUserId,
        channelName: isChannelName,
        sha1: isSha1Hash,
        size: isByteCount
    })
}

export type CreateSignedFileUploadUrlResponse = {
    signedUrl: UrlString
}

export const isCreateSignedFileUploadUrlResponse = (x: any): x is CreateSignedFileUploadUrlResponse => {
    return _validateObject(x, {
        signedUrl: isUrlString
    })
}

export type CreateSignedSubfeedMessageUploadUrlRequestBody = {
    type: 'createSignedSubfeedMessageUploadUrl'
    nodeId: NodeId
    ownerId: UserId
    channelName: ChannelName
    feedId: FeedId
    subfeedHash: SubfeedHash
    messageNumberRange: [number, number]
}

export const isCreateSignedSubfeedMessageUploadUrlRequestBody = (x: any): x is CreateSignedSubfeedMessageUploadUrlRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('createSignedSubfeedMessageUploadUrl'),
        nodeId: isNodeId,
        ownerId: isUserId,
        channelName: isChannelName,
        feedId: isFeedId,
        subfeedHash: isSubfeedHash,
        messageNumberRange: isArrayOf(isNumber)
    })
}

export type CreateSignedSubfeedMessageUploadUrlResponse = {
    signedUrls: {[key: string]: UrlString}
}

export const isCreateSignedSubfeedMessageUploadUrlResponse = (x: any): x is CreateSignedSubfeedMessageUploadUrlResponse => {
    return _validateObject(x, {
        signedUrls: (a => (true))
    })
}

export type CreateSignedTaskResultUploadUrlRequestBody = {
    type: 'createSignedTaskResultUploadUrl'
    nodeId: NodeId
    ownerId: UserId
    channelName: ChannelName
    taskHash: TaskHash
    size: ByteCount
}

export const isCreateSignedTaskResultUploadUrlRequestBody = (x: any): x is CreateSignedTaskResultUploadUrlRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('createSignedTaskResultUploadUrl'),
        nodeId: isNodeId,
        ownerId: isUserId,
        channelName: isChannelName,
        taskHash: isTaskHash,
        size: isByteCount
    })
}

export type CreateSignedTaskResultUploadUrlResponse = {
    signedUrl: UrlString
}

export const isCreateSignedTaskResultUploadUrlResponse = (x: any): x is CreateSignedTaskResultUploadUrlResponse => {
    return _validateObject(x, {
        signedUrl: isUrlString
    })
}

export type KacheryNodeRequestBody =
    ReportRequestBody | GetNodeConfigRequestBody | GetPubsubAuthForChannelRequestBody | CreateSignedFileUploadUrlRequestBody | CreateSignedSubfeedMessageUploadUrlRequestBody | CreateSignedTaskResultUploadUrlRequestBody

export const isKacheryNodeRequestBody = (x: any): x is KacheryNodeRequestBody => {
    return isOneOf([
        isReportRequestBody, isGetNodeConfigRequestBody, isGetPubsubAuthForChannelRequestBody, isCreateSignedFileUploadUrlRequestBody, isCreateSignedSubfeedMessageUploadUrlRequestBody, isCreateSignedTaskResultUploadUrlRequestBody
    ])(x)
}

export type KacheryNodeRequest = {
    body: KacheryNodeRequestBody
    nodeId: NodeId
    signature: Signature
}

export const isKacheryNodeRequest = (x: any): x is KacheryNodeRequest => {
    return _validateObject(x, {
        body: isKacheryNodeRequestBody,
        nodeId: isNodeId,
        signature: isSignature
    })
}