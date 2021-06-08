import { FeedId, FileKey, isEqualTo, isFeedId, isFileKey, isMessageCount, isNodeId, isOneOf, isSignature, isSubfeedHash, MessageCount, NodeId, Signature, subfeedHash, SubfeedHash, _validateObject } from "./kacheryTypes";

export type RequestFileMessageBody = {
    type: 'requestFile',
    fileKey: FileKey
}

export const isRequestFileMessageBody = (x: any): x is RequestFileMessageBody => {
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

export type SubfeedMessageCountUpdateMessageBody = {
    type: 'subfeedMessageCountUpdate',
    feedId: FeedId,
    subfeedHash: SubfeedHash,
    messageCount: MessageCount
}

export const isSubfeedMessageCountUpdateMessageBody = (x: any): x is SubfeedMessageCountUpdateMessageBody => {
    return _validateObject(x, {
        type: isEqualTo('subfeedMessageCountUpdate'),
        feedId: isFeedId,
        subfeedHash: isSubfeedHash,
        messageCount: isMessageCount
    })
}

export type RequestSubfeedMessageBody = {
    type: 'requestSubfeed',
    feedId: FeedId,
    subfeedHash: SubfeedHash
}

export const isRequestSubfeedMessageBody = (x: any): x is RequestSubfeedMessageBody => {
    return _validateObject(x, {
        type: isEqualTo('requestSubfeed'),
        feedId: isFeedId,
        subfeedHash: isSubfeedHash
    })
}

export type KacheryHubPubsubMessageBody = RequestFileMessageBody | UploadFileStatusMessageBody | SubfeedMessageCountUpdateMessageBody | RequestSubfeedMessageBody

export const isKacheryHubPubsubMessageBody = (x: any): x is KacheryHubPubsubMessageBody => {
    return isOneOf([
        isRequestFileMessageBody,
        isUploadFileStatusMessageBody,
        isSubfeedMessageCountUpdateMessageBody,
        isRequestSubfeedMessageBody
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