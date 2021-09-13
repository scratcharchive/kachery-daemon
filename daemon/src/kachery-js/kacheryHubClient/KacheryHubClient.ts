import Ably from 'ably'
import BitwooderDelegationCert from 'kachery-js/types/BitwooderDelegationCert'
import { hexToPrivateKey, hexToPublicKey, signMessage, verifySignature } from '../crypto/signatures'
import { BitwooderResourceRequest, BitwooderResourceResponse, GetAblyTokenRequestRequest, GetUploadUrlRequest } from '../types/BitwooderResourceRequest'
import { PubsubAuth } from "../types/kacheryHubTypes"
import { CreateSignedSubfeedMessageUploadUrlRequestBody, CreateSignedTaskResultUploadUrlRequestBody, GetBitwooderCertForChannelRequestBody, GetChannelConfigRequestBody, GetNodeConfigRequestBody, isCreateSignedSubfeedMessageUploadUrlResponse, isCreateSignedTaskResultUploadUrlResponse, isGetBitwooderCertForChannelResponse, isGetChannelConfigResponse, isGetNodeConfigResponse, KacheryNodeRequestBody, ReportRequestBody } from "../types/kacheryNodeRequestTypes"
import { ByteCount, ChannelName, FeedId, JSONValue, NodeId, nodeIdToPublicKeyHex, NodeLabel, PrivateKeyHex, PubsubChannelName, Sha1Hash, SubfeedHash, TaskId, urlString, UserId } from "../types/kacheryTypes"
import { isKacheryHubPubsubMessageData, KacheryHubPubsubMessageBody } from '../types/pubsubMessages'
import randomAlphaString from '../util/randomAlphaString'
import { AblyAuthCallback, AblyAuthCallbackCallback } from "./AblyPubsubClient"
import createPubsubClient, { PubsubClient, PubsubMessage } from "./createPubsubClient"

export type IncomingKacheryHubPubsubMessage = {
    channelName: ChannelName,
    pubsubChannelName: PubsubChannelName,
    fromNodeId: NodeId,
    message: KacheryHubPubsubMessageBody
}

const minuteMsec = 1000 * 60

class KacheryHubClient {
    #pubsubClients: {[key: string]: PubsubClient} = {}
    #incomingPubsubMessageCallbacks: {[key: string]: (x: IncomingKacheryHubPubsubMessage) => void} = {}
    #bitwooderCertsByChannel: {[key: string]: {cert: BitwooderDelegationCert, key: PrivateKeyHex}} = {}
    constructor(private opts: {
        nodeId: NodeId,
        sendKacheryNodeRequest: (message: KacheryNodeRequestBody) => Promise<JSONValue>,
        sendBitwooderResourceRequest: (request: BitwooderResourceRequest) => Promise<BitwooderResourceResponse>,
        ownerId?: UserId,
        nodeLabel?: NodeLabel,
        kacheryHubUrl: string
        bitwooderUrl: string
    }) {
    }
    async fetchNodeConfig() {
        if (!this.opts.ownerId) throw Error('No owner ID in fetchNodeConfig')
        const reqBody: GetNodeConfigRequestBody = {
            type: 'getNodeConfig',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId
        }
        const resp = await this._sendRequest(reqBody)
        if (!isGetNodeConfigResponse(resp)) {
            throw Error('Invalid response in getNodeConfig')
        }
        if (!resp.found) {
            throw Error('Node not found for getNodeConfig')
        }
        const nodeConfig = resp.nodeConfig
        if (!nodeConfig) throw Error('Unexpected, no nodeConfig')
        return nodeConfig
    }
    async fetchChannelConfig(channelName: ChannelName) {
        const reqBody: GetChannelConfigRequestBody = {
            type: 'getChannelConfig',
            channelName
        }
        const resp = await this._sendRequest(reqBody)
        if (!isGetChannelConfigResponse(resp)) {
            throw Error('Invalid response in getChannelConfig')
        }
        if (!resp.found) {
            throw Error('Channel not found for getChannelConfig')
        }
        const channelConfig = resp.channelConfig
        if (!channelConfig) throw Error('Unexpected, no channelConfig')
        return channelConfig
    }
    async getBitwooderCertForChannel(channelName: ChannelName): Promise<{cert: BitwooderDelegationCert, key: PrivateKeyHex}> {
        if (!this.opts.ownerId) throw Error('No owner ID in getBitwooderCertForChannel')
        let a: {cert: BitwooderDelegationCert, key: PrivateKeyHex} | undefined = this.#bitwooderCertsByChannel[channelName.toString()]
        if (a) {
            if (a.cert.payload.expires < Date.now() + 4000) { // give the expiration a buffer
                a = undefined
            }
        }
        if (a) return a
        // todo: there is a race condition - we may make several requests before we find it in the memory cache
        const reqBody: GetBitwooderCertForChannelRequestBody = {
            type: 'getBitwooderCertForChannel',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId,
            channelName
        }
        const resp = await this._sendRequest(reqBody)
        if (!isGetBitwooderCertForChannelResponse(resp)) {
            throw Error('Invalid response in getBitwooderCertForChannel')
        }
        a = {cert: resp.cert, key: resp.key}
        this.#bitwooderCertsByChannel[channelName.toString()] = a
        return a
    }
    async fetchPubsubAuthForChannel(channelName: ChannelName): Promise<PubsubAuth> {
        if (!this.opts.ownerId) throw Error('No owner ID in fetchPubsubAuthForChannel')
        const channelConfig = await this.fetchChannelConfig(channelName)
        const resourceId = channelConfig.bitwooderResourceId
        if (!resourceId) {
            throw Error('No bitwooderResourceId in channel config (fetchPubsubAuthForChannel)')
        }
        const {cert: bitwooderCert, key: bitwooderCertKey} = await this.getBitwooderCertForChannel(channelName)
        const ablyCapability = bitwooderCert.payload.attributes.ablyCapability
        if (!ablyCapability) {
            throw Error('No ablyCapability in bitwooder cert')
        }
        const payload = {
            type: 'getAblyTokenRequest' as 'getAblyTokenRequest',
            expires: Date.now() + minuteMsec * 1,
            resourceId,
            capability: ablyCapability
        }
        const keyPair = {
            publicKey: hexToPublicKey(bitwooderCert.payload.delegatedSignerId),
            privateKey: hexToPrivateKey(bitwooderCertKey)
        }
        const req: GetAblyTokenRequestRequest = {
            type: 'getAblyTokenRequest',
            payload,
            auth: {
                signerId: bitwooderCert.payload.delegatedSignerId,
                signature: await signMessage(payload, keyPair),
                delegationCertificate: bitwooderCert
            }
        }
        const resp: BitwooderResourceResponse = await this._sendRequestToBitwooder(req)
        if (resp.type !== 'getAblyTokenRequest') {
            throw Error('Unexpected response type for getAblyTokenRequest')
        }
        return {
            ablyTokenRequest: resp.ablyTokenRequest
        }

        // const reqBody: GetPubsubAuthForChannelRequestBody = {
        //     type: 'getPubsubAuthForChannel',
        //     nodeId: this.nodeId,
        //     ownerId: this.opts.ownerId,
        //     channelName
        // }
        // const pubsubAuth = await this._sendRequest(reqBody)
        // if (!isPubsubAuth(pubsubAuth)) {
        //     console.warn(pubsubAuth)
        //     throw Error('Invalid pubsub auth')
        // }
        // return pubsubAuth
    }
    async report() {
        if (!this.opts.ownerId) throw Error('No owner ID in report')
        if (!this.opts.nodeLabel) throw Error('No node label in report')
        const reqBody: ReportRequestBody = {
            type: 'report',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId,
            nodeLabel: this.opts.nodeLabel
        }
        await this._sendRequest(reqBody)
    }
    async createSignedFileUploadUrl(a: {channelName: ChannelName, sha1: Sha1Hash, size: ByteCount}) {
        if (!this.opts.ownerId) throw Error('No owner ID in createSignedFileUploadUrl')
        const {channelName, sha1, size} = a

        const channelConfig = await this.fetchChannelConfig(channelName)
        const resourceId = channelConfig.bitwooderResourceId
        if (!resourceId) {
            throw Error('No bitwooderResourceId in channel config (createSignedFileUploadUrl)')
        }

        const s = sha1
        const filePath = `${channelName}/sha1/${s[0]}${s[1]}/${s[2]}${s[3]}/${s[4]}${s[5]}/${s}`
        
        const {cert: bitwooderCert, key: bitwooderCertKey} = await this.getBitwooderCertForChannel(channelName)

        const payload = {
            type: 'getUploadUrl' as 'getUploadUrl',
            expires: Date.now() + minuteMsec * 1,
            resourceId,
            filePath, 
            size: Number(size)
        }

        const keyPair = {
            publicKey: hexToPublicKey(bitwooderCert.payload.delegatedSignerId),
            privateKey: hexToPrivateKey(bitwooderCertKey)
        }
        const req: GetUploadUrlRequest = {
            type: 'getUploadUrl',
            payload,
            auth: {
                signerId: bitwooderCert.payload.delegatedSignerId,
                signature: await signMessage(payload, keyPair),
                delegationCertificate: bitwooderCert
            }
        }

        const resp: BitwooderResourceResponse = await this._sendRequestToBitwooder(req)
        if (resp.type !== 'getUploadUrl') {
            throw Error('Unexpected response type for getUploadUrl')
        }
        return urlString(resp.uploadUrl)
        
        // const {channelName, sha1, size} = a
        // const reqBody: CreateSignedFileUploadUrlRequestBody = {
        //     type: 'createSignedFileUploadUrl',
        //     nodeId: this.nodeId,
        //     ownerId: this.opts.ownerId,
        //     channelName,
        //     sha1,
        //     size
        // }
        // const x = await this._sendRequest(reqBody)
        // if (!isCreateSignedFileUploadUrlResponse(x)) {
        //     throw Error('Unexpected response for createSignedFileUploadUrl')
        // }
        // return x.signedUrl
    }
    async createSignedSubfeedMessageUploadUrls(a: {channelName: ChannelName, feedId: FeedId, subfeedHash: SubfeedHash, messageNumberRange: [number, number]}) {
        if (!this.opts.ownerId) throw Error('No owner ID in createSignedSubfeedMessageUploadUrls')
        const {channelName, feedId, subfeedHash, messageNumberRange} = a
        const reqBody: CreateSignedSubfeedMessageUploadUrlRequestBody = {
            type: 'createSignedSubfeedMessageUploadUrl',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId,
            channelName,
            feedId,
            subfeedHash,
            messageNumberRange
        }
        const x = await this._sendRequest(reqBody)
        if (!isCreateSignedSubfeedMessageUploadUrlResponse(x)) {
            throw Error('Unexpected response for createSignedFileUploadUrl')
        }
        return x.signedUrls
    }
    async createSignedTaskResultUploadUrl(a: {channelName: ChannelName, taskId: TaskId, size: ByteCount}) {
        if (!this.opts.ownerId) throw Error('No owner ID in createSignedTaskResultUploadUrl')
        const {channelName, taskId, size} = a
        const reqBody: CreateSignedTaskResultUploadUrlRequestBody = {
            type: 'createSignedTaskResultUploadUrl',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId,
            channelName,
            taskId,
            size
        }
        const x = await this._sendRequest(reqBody)
        if (!isCreateSignedTaskResultUploadUrlResponse(x)) {
            throw Error('Unexpected response for createSignedTaskResultUploadUrl')
        }
        return x.signedUrl
    }
    public get nodeId() {
        return this.opts.nodeId
    }
    clearPubsubClientsForChannels() {
        for (let k in this.#pubsubClients) {
            const client = this.#pubsubClients[k]
            client.unsubscribe()
        }
        this.#pubsubClients = {}
    }
    createPubsubClientForChannel(channelName: ChannelName, subscribeToPubsubChannels: PubsubChannelName[]) {
        if (channelName.toString() in this.#pubsubClients) {
            // todo: think about how to update the subscriptions of the auth has changed
            return
        }
        const ablyAuthCallback: AblyAuthCallback = (tokenParams: Ably.Types.TokenParams, callback: AblyAuthCallbackCallback) => {
            // We ignore tokenParams because the capabilities are determined on the server side
            this.fetchPubsubAuthForChannel(channelName).then((auth: PubsubAuth) => {
                callback('', auth.ablyTokenRequest)
            }).catch((err: Error) => {
                callback(err.message, '')
            })
        }
        const client = createPubsubClient({ably: {ablyAuthCallback}})
        for (let pubsubChannelName of subscribeToPubsubChannels) {
            client.getChannel(pubsubChannelName).subscribe((msg: PubsubMessage) => {
                const messageData = msg.data
                if (isKacheryHubPubsubMessageData(messageData)) {
                    const publicKey = hexToPublicKey(nodeIdToPublicKeyHex(messageData.fromNodeId))
                    verifySignature(messageData.body as any as JSONValue, publicKey, messageData.signature).then(verified => {
                        if (!verified) {
                            console.warn(messageData)
                            console.warn(`Problem verifying signature on pubsub message: channel=${channelName} pubsubChannelName=${pubsubChannelName}`, messageData.fromNodeId)
                            return
                        }
                        for (let k in this.#incomingPubsubMessageCallbacks) {
                            const cb = this.#incomingPubsubMessageCallbacks[k]
                            cb({
                                channelName,
                                pubsubChannelName,
                                fromNodeId: messageData.fromNodeId,
                                message: messageData.body
                            })
                        }
                    }).catch(err => {
                        console.log('Problem verifying signature on pubsub message', err)
                    })
                }
                else {
                    console.warn(`Invalid pubsub message data: channel=${channelName}, pubsubChannel=${pubsubChannelName}`)
                }
            })
        }
        this.#pubsubClients[channelName.toString()] = client
    }
    getPubsubClientForChannel(channelName: ChannelName) {
        if (channelName.toString() in this.#pubsubClients) {
            return this.#pubsubClients[channelName.toString()]
        }
        else {
            return undefined
        }
    }
    onIncomingPubsubMessage(cb: (x: IncomingKacheryHubPubsubMessage) => void) {
        const k = randomAlphaString(10)
        this.#incomingPubsubMessageCallbacks[k] = cb
        return {cancel: () => {
            if (this.#incomingPubsubMessageCallbacks[k]) {
                delete this.#incomingPubsubMessageCallbacks[k]
            }
        }}
    }
    async _sendRequest(requestBody: KacheryNodeRequestBody): Promise<JSONValue> {
        return await this.opts.sendKacheryNodeRequest(requestBody)
    }
    async _sendRequestToBitwooder(request: BitwooderResourceRequest): Promise<BitwooderResourceResponse> {
        return await this.opts.sendBitwooderResourceRequest(request)
    }
    _kacheryHubUrl() {
        return this.opts.kacheryHubUrl
    }
    _bitwooderUrl() {
        return this.opts.bitwooderUrl
    }
}

export default KacheryHubClient
