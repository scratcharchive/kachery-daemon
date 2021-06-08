import { isNodeConfig, NodeConfig } from "./kacheryHubTypes"
import { ByteCount, FeedId, isBoolean, isByteCount, isEqualTo, isFeedId, isNodeId, isNodeLabel, isNumber, isOneOf, isSha1Hash, isSignature, isString, isSubfeedHash, isUrlString, NodeId, NodeLabel, optional, Sha1Hash, Signature, SubfeedHash, UrlString, _validateObject } from "./kacheryTypes"

export type ReportRequestBody = {
    type: 'report'
    nodeId: NodeId
    ownerId: string
    nodeLabel: NodeLabel
}

export const isReportRequestBody = (x: any): x is ReportRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('report'),
        nodeId: isNodeId,
        ownerId: isString,
        nodeLabel: isNodeLabel
    })
}

export type GetNodeConfigRequestBody = {
    type: 'getNodeConfig'
    nodeId: NodeId
    ownerId: string
}

export const isGetNodeConfigRequestBody = (x: any): x is GetNodeConfigRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('getNodeConfig'),
        nodeId: isNodeId,
        ownerId: isString
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
    ownerId: string,
    channelName: string
}

export const isGetPubsubAuthForChannelRequestBody = (x: any): x is GetPubsubAuthForChannelRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('getPubsubAuthForChannel'),
        nodeId: isNodeId,
        ownerId: isString,
        channelName: isString
    })
}

export type CreateSignedFileUploadUrlRequestBody = {
    type: 'createSignedFileUploadUrl'
    nodeId: NodeId
    ownerId: string
    channelName: string
    sha1: Sha1Hash
    size: ByteCount
}

export const isCreateSignedFileUploadUrlRequestBody = (x: any): x is CreateSignedFileUploadUrlRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('createSignedFileUploadUrl'),
        nodeId: isNodeId,
        ownerId: isString,
        channelName: isString,
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
    ownerId: string
    channelName: string
    feedId: FeedId
    subfeedHash: SubfeedHash
    messageNumber?: number
    subfeedJson?: boolean
}

export const isCreateSignedSubfeedMessageUploadUrlRequestBody = (x: any): x is CreateSignedSubfeedMessageUploadUrlRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('createSignedSubfeedMessageUploadUrl'),
        nodeId: isNodeId,
        ownerId: isString,
        channelName: isString,
        feedId: isFeedId,
        subfeedHash: isSubfeedHash,
        messageNumber: optional(isNumber),
        subfeedJson: optional(isBoolean)
    })
}

export type CreateSignedSubfeedMessageUploadUrlResponse = {
    signedUrl: UrlString
}

export const isCreateSignedSubfeedMessageUploadUrlResponse = (x: any): x is CreateSignedSubfeedMessageUploadUrlResponse => {
    return _validateObject(x, {
        signedUrl: isUrlString
    })
}

export type KacheryNodeRequestBody =
    ReportRequestBody | GetNodeConfigRequestBody | GetPubsubAuthForChannelRequestBody | CreateSignedFileUploadUrlRequestBody | CreateSignedSubfeedMessageUploadUrlRequestBody

export const isKacheryNodeRequestBody = (x: any): x is KacheryNodeRequestBody => {
    return isOneOf([
        isReportRequestBody, isGetNodeConfigRequestBody, isGetPubsubAuthForChannelRequestBody, isCreateSignedFileUploadUrlRequestBody, isCreateSignedSubfeedMessageUploadUrlRequestBody
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