import { ByteCount, isByteCount, isEqualTo, isNodeId, isNodeLabel, isOneOf, isSha1Hash, isSignature, isString, isUrlString, NodeId, NodeLabel, Sha1Hash, Signature, UrlString, _validateObject } from "./kacheryTypes"

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

export type CreateSignedUploadUrlRequestBody = {
    type: 'createSignedUploadUrl'
    nodeId: NodeId
    ownerId: string
    bucketUri: string
    sha1: Sha1Hash
    size: ByteCount
}

export const isCreateSignedUploadUrlRequestBody = (x: any): x is CreateSignedUploadUrlRequestBody => {
    return _validateObject(x, {
        type: isEqualTo('createSignedUploadUrl'),
        nodeId: isNodeId,
        ownerId: isString,
        bucketUri: isString,
        sha1: isSha1Hash,
        size: isByteCount
    })
}

export type CreateSignedUploadUrlResponse = {
    signedUrl: UrlString
}

export const isCreateSignedUploadUrlResponse = (x: any): x is CreateSignedUploadUrlResponse => {
    return _validateObject(x, {
        signedUrl: isUrlString
    })
}

export type KacheryNodeRequestBody =
    ReportRequestBody | GetNodeConfigRequestBody | GetPubsubAuthForChannelRequestBody | CreateSignedUploadUrlRequestBody

export const isKacheryNodeRequestBody = (x: any): x is KacheryNodeRequestBody => {
    return isOneOf([
        isReportRequestBody, isGetNodeConfigRequestBody, isGetPubsubAuthForChannelRequestBody, isCreateSignedUploadUrlRequestBody
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