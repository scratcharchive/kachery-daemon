import { FileKey, isEqualTo, isFileKey, isNodeId, isOneOf, isSignature, NodeId, Signature, _validateObject } from "./kacheryTypes";

export type RequestFilePubsubMessageBody = {
    type: 'requestFile',
    fileKey: FileKey
}

export const isRequestFilePubsubMessageBody = (x: any): x is RequestFilePubsubMessageBody => {
    return _validateObject(x, {
        type: isEqualTo('requestFile'),
        fileKey: isFileKey
    })
}

export type UploadFileStatusMessageBody = {
    type: 'uploadFileStatus',
    fileKey: FileKey,
    status: 'started' | 'finished'
}

export const isUploadFileStatusMessageBody = (x: any): x is UploadFileStatusMessageBody => {
    return _validateObject(x, {
        type: isEqualTo('uploadFileStatus'),
        fileKey: isFileKey,
        status: isOneOf(['started', 'finished'].map(s => isEqualTo(s)))
    })
}

export type KacheryHubPubsubMessageBody = RequestFilePubsubMessageBody | UploadFileStatusMessageBody

export const isKacheryHubPubsubMessageBody = (x: any): x is KacheryHubPubsubMessageBody => {
    return isOneOf([
        isRequestFilePubsubMessageBody,
        isUploadFileStatusMessageBody
    ])(x)
}

export type KacheryHubPubsubMessageData = {
    body: KacheryHubPubsubMessageBody,
    fromNodeId: NodeId,
    signature: Signature
}

export const isKacheryHubPubsubMessageData = (x: any): x is KacheryHubPubsubMessageData => {
    return _validateObject(x, {
        body: isKacheryHubPubsubMessageBody,
        fromNodeId: isNodeId,
        signature: isSignature
    })
}