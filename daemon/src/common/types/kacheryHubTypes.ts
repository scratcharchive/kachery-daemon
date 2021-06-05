import { isArrayOf, isBoolean, isEqualTo, isNodeId, isNodeLabel, isNumber, isOneOf, isSignature, isString, isTimestamp, NodeId, NodeLabel, optional, Signature, Timestamp, _validateObject } from "./kacheryTypes"

export type GoogleServiceAccountCredentials = {
    type: 'service_account',
    project_id: string,
    private_key_id: string,
    private_key: string,
    client_email: string,
    client_id: string
}

export const isGoogleServiceAccountCredentials = (x: any): x is GoogleServiceAccountCredentials => {
    return _validateObject(x, {
        type: isEqualTo('service_account'),
        project_id: isString,
        private_key_id: isString,
        private_key: isString,
        client_email: isString,
        client_id: isString,
    }, {allowAdditionalFields: true})
}

export interface AblyTokenRequest {
    capability: string
    clientId?: string
    keyName: string
    mac: string
    nonce: string
    timestamp: number
    ttl?: number
}
export const isAblyTokenRequest = (x: any): x is AblyTokenRequest => {
    return _validateObject(x, {
        capability: isString,
        keyName: isString,
        mac: isString,
        nonce: isString,
        timestamp: isNumber
    }, {allowAdditionalFields: true})
}

export type PubsubAuth = {
    ablyTokenRequest: AblyTokenRequest
}
export const isPubsubAuth = (x: any): x is PubsubAuth => {
    return _validateObject(x, {
        ablyTokenRequest: isAblyTokenRequest
    })
}

export type NodeChannelAuthorization = {
    channelName: string
    nodeId: NodeId
    permissions: {
        requestFiles?: boolean
        requestFeeds?: boolean
        requestTaskResults?: boolean
        provideFiles?: boolean
        provideFeeds?: boolean
        provideTaskResults?: boolean
    }
}

export const isNodeChannelAuthorization = (x: any): x is NodeChannelAuthorization => {
    return _validateObject(x, {
        channelName: isString,
        nodeId: isNodeId,
        permissions: {
            downloadFiles: optional(isBoolean),
            downloadFeeds: optional(isBoolean),
            downloadTaskResults: optional(isBoolean),
            requestFiles: optional(isBoolean),
            requestFeeds: optional(isBoolean),
            requestTaskResults: optional(isBoolean),
            provideFiles: optional(isBoolean),
            provideFeeds: optional(isBoolean),
            provideTaskResults: optional(isBoolean)
        }
    })
}

export type ChannelConfig = {
    channelName: string
    ownerId: string
    bucketUri?: string
    googleServiceAccountCredentials?: string | '*private*'
    ablyApiKey?: string | '*private*'
    deleted?: boolean
    authorizedNodes?: NodeChannelAuthorization[]
}

export const isChannelConfig = (x: any): x is ChannelConfig => {
    return _validateObject(x, {
        channelName: isString,
        ownerId: isString,
        bucketUri: optional(isString),
        googleServiceAccountCredentials: optional(isOneOf([isString, isEqualTo('*private*')])),
        ablyApiKey: optional(isOneOf([isString, isEqualTo('*private*')])),
        deleted: optional(isBoolean),
        authorizedNodes: optional(isArrayOf(isNodeChannelAuthorization))
    })
}

export type NodeReport = {
    nodeId: NodeId,
    ownerId: string,
    nodeLabel: NodeLabel
}

export const isNodeReport = (x: any): x is NodeReport => {
    return _validateObject(x, {
        nodeId: isNodeId,
        ownerId: isString,
        nodeLabel: isNodeLabel
    }, {allowAdditionalFields: true})
}

export type NodeChannelMembership = {
    nodeId: NodeId
    channelName: string
    roles: {
        downloadFiles?: boolean
        downloadFeeds?: boolean
        downloadTaskResults?: boolean
        requestFiles?: boolean
        requestFeeds?: boolean
        requestTaskResults?: boolean
        provideFiles?: boolean
        provideFeeds?: boolean
        provideTaskResults?: boolean
    }
    channelBucketUri?: string // obtained by cross-referencing the channels collection
    authorization?: NodeChannelAuthorization // obtained by cross-referencing the channels collection
}

const isNodeChannelMembership = (x: any): x is NodeChannelMembership => {
    return _validateObject(x, {
        nodeId: isNodeId,
        channelName: isString,
        roles: {
            downloadFiles: optional(isBoolean),
            downloadFeeds: optional(isBoolean),
            downloadTaskResults: optional(isBoolean),
            requestFiles: optional(isBoolean),
            requestFeeds: optional(isBoolean),
            requestTaskResults: optional(isBoolean),
            provideFiles: optional(isBoolean),
            provideFeeds: optional(isBoolean),
            provideTaskResults: optional(isBoolean)
        },
        channelBucketUri: optional(isString),
        authorization: optional(isNodeChannelAuthorization)
    })
}

export type NodeConfig = {
    nodeId: NodeId
    ownerId: string
    channelMemberships?: NodeChannelMembership[]
    lastNodeReport?: NodeReport
    lastNodeReportTimestamp?: Timestamp
    deleted?: boolean
}

export const isNodeConfig = (x: any): x is NodeConfig => {
    return _validateObject(x, {
        nodeId: isNodeId,
        ownerId: isString,
        channelMemberships: optional(isArrayOf(isNodeChannelMembership)),
        memberships: optional(isNumber), // for historical - remove eventually
        lastNodeReport: optional(isNodeReport),
        lastNodeReportTimestamp: optional(isTimestamp),
        deleted: optional(isBoolean)
    })
}

export type Auth = {
    userId?: string,
    googleIdToken?: string
}

export const isAuth = (x: any): x is Auth => {
    return _validateObject(x, {
            userId: optional(isString),
            googleIdToken: optional(isString)
    })
}

export type GetChannelsForUserRequest = {
    type: 'getChannelsForUser'
    userId: string
    auth: Auth
}

export const isGetChannelsForUserRequest = (x: any): x is GetChannelsForUserRequest => {
    return _validateObject(x, {
        type: isEqualTo('getChannelsForUser'),
        userId: isString,
        auth: isAuth
    })
}

export type AddChannelRequest = {
    type: 'addChannel'
    channel: ChannelConfig
    auth: Auth
}

export const isAddChannelRequest = (x: any): x is AddChannelRequest => {
    return _validateObject(x, {
        type: isEqualTo('addChannel'),
        channel: isChannelConfig,
        auth: isAuth
    })
}

export type DeleteChannelRequest = {
    type: 'deleteChannel'
    channelName: string
    auth: Auth
}

export const isDeleteChannelRequest = (x: any): x is DeleteChannelRequest => {
    return _validateObject(x, {
        type: isEqualTo('deleteChannel'),
        channelName: isString,
        auth: isAuth
    })
}

export type GetNodesForUserRequest = {
    type: 'getNodesForUser'
    userId: string,
    auth: Auth
}

export const isGetNodesForUserRequest = (x: any): x is GetNodesForUserRequest => {
    return _validateObject(x, {
        type: isEqualTo('getNodesForUser'),
        userId: isString,
        auth: isAuth
    })
}

export type GetNodeForUserRequest = {
    type: 'getNodeForUser'
    nodeId: NodeId
    userId: string
    auth: Auth
}

export const isGetNodeForUserRequest = (x: any): x is GetNodeForUserRequest => {
    return _validateObject(x, {
        type: isEqualTo('getNodeForUser'),
        nodeId: isNodeId,
        userId: isString,
        auth: isAuth
    })
}

export type GetChannelRequest = {
    type: 'getChannel'
    channelName: string
    auth: Auth
}

export const isGetChannelRequest = (x: any): x is GetChannelRequest => {
    return _validateObject(x, {
        type: isEqualTo('getChannel'),
        channelName: isString,
        auth: isAuth
    })
}

export type AddNodeRequest = {
    type: 'addNode'
    node: NodeConfig
    auth: Auth
}

export const isAddNodeRequest = (x: any): x is AddNodeRequest => {
    return _validateObject(x, {
        type: isEqualTo('addNode'),
        node: isNodeConfig,
        auth: isAuth
    })
}

export type DeleteNodeRequest = {
    type: 'deleteNode'
    nodeId: NodeId
    auth: Auth
}

export const isDeleteNodeRequest = (x: any): x is DeleteNodeRequest => {
    return _validateObject(x, {
        type: isEqualTo('deleteNode'),
        nodeId: isNodeId,
        auth: isAuth
    })
}

export type AddNodeChannelMembershipRequest = {
    type: 'addNodeChannelMembership',
    nodeId: NodeId
    channelName: string
    auth: Auth
}

export const isAddNodeChannelMembershipRequest = (x: any): x is AddNodeChannelMembershipRequest => {
    return _validateObject(x, {
        type: isEqualTo('addNodeChannelMembership'),
        nodeId: isNodeId,
        channelName: isString,
        auth: isAuth
    })
}

export type AddAuthorizedNodeRequest = {
    type: 'addAuthorizedNode'
    channelName: string
    nodeId: NodeId
    auth: Auth
}

export const isAddAuthorizedNodeRequest = (x: any): x is AddAuthorizedNodeRequest => {
    return _validateObject(x, {
        type: isEqualTo('addAuthorizedNode'),
        channelName: isString,
        nodeId: isNodeId,
        auth: isAuth
    })
}

export type UpdateNodeChannelAuthorizationRequest = {
    type: 'updateNodeChannelAuthorization'
    authorization: NodeChannelAuthorization
    auth: Auth
}

export const isUpdateNodeChannelAuthorizationRequest = (x: any): x is UpdateNodeChannelAuthorizationRequest => {
    return _validateObject(x, {
        type: isEqualTo('updateNodeChannelAuthorization'),
        authorization: isNodeChannelAuthorization,
        auth: isAuth
    })
}

export type DeleteNodeChannelAuthorizationRequest = {
    type: 'deleteNodeChannelAuthorization'
    channelName: string
    nodeId: NodeId
    auth: Auth
}

export const isDeleteNodeChannelAuthorizationRequest = (x: any): x is DeleteNodeChannelAuthorizationRequest => {
    return _validateObject(x, {
        type: isEqualTo('deleteNodeChannelAuthorization'),
        channelName: isString,
        nodeId: isNodeId,
        auth: isAuth
    })
}

export type UpdateChannelPropertyRequest = {
    type: 'updateChannelProperty'
    channelName: string
    propertyName: 'bucketUri' | 'ablyApiKey' | 'googleServiceAccountCredentials'
    propertyValue: string
    auth: Auth
}

export const isUpdateChannelPropertyRequest = (x: any): x is UpdateChannelPropertyRequest => {
    return _validateObject(x, {
        type: isEqualTo('updateChannelProperty'),
        channelName: isString,
        propertyName: isOneOf(['bucketUri', 'ablyApiKey', 'googleServiceAccountCredentials'].map(x => isEqualTo(x))),
        propertyValue: isString,
        auth: isAuth
    })
}

export type UpdateNodeChannelMembershipRequest = {
    type: 'updateNodeChannelMembership'
    membership: NodeChannelMembership
    auth: Auth
}

export const isUpdateNodeChannelMembershipRequest = (x: any): x is UpdateNodeChannelMembershipRequest => {
    return _validateObject(x, {
        type: isEqualTo('updateNodeChannelMembership'),
        membership: isNodeChannelMembership,
        auth: isAuth
    })
}

export type DeleteNodeChannelMembershipRequest = {
    type: 'deleteNodeChannelMembership'
    channelName: string
    nodeId: NodeId
    auth: Auth
}

export const isDeleteNodeChannelMembershipRequest = (x: any): x is DeleteNodeChannelMembershipRequest => {
    return _validateObject(x, {
        type: isEqualTo('deleteNodeChannelMembership'),
        channelName: isString,
        nodeId: isNodeId,
        auth: isAuth
    })
}

export type NodeReportRequestBody = {
    nodeId: NodeId
    ownerId: string
    nodeLabel: NodeLabel
}

export const isNodeReportRequestBody = (x: any): x is NodeReportRequestBody => {
    return _validateObject(x, {
        nodeId: isNodeId,
        ownerId: isString,
        nodeLabel: isNodeLabel
    })
}

export type NodeReportRequest = {
    type: 'nodeReport'
    body: NodeReportRequestBody
    signature: Signature
}

export const isNodeReportRequest = (x: any): x is NodeReportRequest => {
    return _validateObject(x, {
        type: isEqualTo('nodeReport'),
        body: isNodeReportRequestBody,
        signature: isSignature
    })
}

export type KacheryHubRequest =
    AddAuthorizedNodeRequest |
    AddChannelRequest |
    AddNodeRequest |
    AddNodeChannelMembershipRequest |
    DeleteChannelRequest |
    DeleteNodeRequest |
    DeleteNodeChannelMembershipRequest |
    DeleteNodeChannelAuthorizationRequest |
    GetChannelRequest |
    GetChannelsForUserRequest |
    GetNodeForUserRequest | 
    GetNodesForUserRequest |
    UpdateChannelPropertyRequest |
    UpdateNodeChannelMembershipRequest |
    UpdateNodeChannelAuthorizationRequest

export const isKacheryHubRequest = (x: any): x is KacheryHubRequest => {
    return isOneOf([
        isAddAuthorizedNodeRequest,
        isAddChannelRequest,
        isAddNodeRequest,
        isAddNodeChannelMembershipRequest,
        isDeleteChannelRequest,
        isDeleteNodeRequest,
        isDeleteNodeChannelMembershipRequest,
        isDeleteNodeChannelAuthorizationRequest,
        isGetChannelRequest,
        isGetChannelsForUserRequest,
        isGetNodeForUserRequest, 
        isGetNodesForUserRequest,
        isUpdateChannelPropertyRequest,
        isUpdateNodeChannelMembershipRequest,
        isUpdateNodeChannelAuthorizationRequest
    ])(x)
}
