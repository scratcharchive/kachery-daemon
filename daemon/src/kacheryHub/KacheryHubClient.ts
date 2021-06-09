import Ably from 'ably'
import axios from "axios"
import { getSignature, nodeIdToPublicKey, publicKeyHexToNodeId, publicKeyToHex, verifySignature } from "../common/types/crypto_util"
import { isNodeConfig, isPubsubAuth, PubsubAuth } from "../common/types/kacheryHubTypes"
import { CreateSignedFileUploadUrlRequestBody, CreateSignedSubfeedMessageUploadUrlRequestBody, GetNodeConfigRequestBody, GetPubsubAuthForChannelRequestBody, isCreateSignedFileUploadUrlResponse, isCreateSignedSubfeedMessageUploadUrlResponse, isGetNodeConfigResponse, KacheryNodeRequest, KacheryNodeRequestBody, ReportRequestBody } from "../common/types/kacheryNodeRequestTypes"
import { ByteCount, ChannelName, FeedId, JSONValue, KeyPair, NodeId, NodeLabel, PubsubChannelName, Sha1Hash, SubfeedHash, UserId } from "../common/types/kacheryTypes"
import { isKacheryHubPubsubMessageData, KacheryHubPubsubMessageBody } from '../common/types/pubsubMessages'
import { randomAlphaString } from "../common/util"
import { AblyAuthCallback, AblyAuthCallbackCallback } from "./AblyPubsubClient"
import createPubsubClient, { PubsubClient, PubsubMessage } from "./createPubsubClient"

export type IncomingKacheryHubPubsubMessage = {
    channelName: ChannelName,
    pubsubChannelName: PubsubChannelName,
    fromNodeId: NodeId,
    message: KacheryHubPubsubMessageBody
}

class KacheryHubClient {
    #pubsubClients: {[key: string]: PubsubClient} = {}
    #incomingPubsubMessageCallbacks: {[key: string]: (x: IncomingKacheryHubPubsubMessage) => void} = {}
    constructor(private opts: {keyPair: KeyPair, ownerId?: UserId, nodeLabel: NodeLabel, kacheryHubUrl: string}) {
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
    async fetchPubsubAuthForChannel(channelName: ChannelName) {
        if (!this.opts.ownerId) throw Error('No owner ID in fetchPubsubAuthForChannel')
        const reqBody: GetPubsubAuthForChannelRequestBody = {
            type: 'getPubsubAuthForChannel',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId,
            channelName
        }
        const pubsubAuth = await this._sendRequest(reqBody)
        if (!isPubsubAuth(pubsubAuth)) {
            console.warn(pubsubAuth)
            throw Error('Invalid pubsub auth')
        }
        return pubsubAuth
    }
    async report() {
        if (!this.opts.ownerId) throw Error('No owner ID in report')
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
        const reqBody: CreateSignedFileUploadUrlRequestBody = {
            type: 'createSignedFileUploadUrl',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId,
            channelName,
            sha1,
            size
        }
        const x = await this._sendRequest(reqBody)
        if (!isCreateSignedFileUploadUrlResponse(x)) {
            throw Error('Unexpected response for createSignedFileUploadUrl')
        }
        return x.signedUrl
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
    public get nodeId() {
        return publicKeyHexToNodeId(publicKeyToHex(this.opts.keyPair.publicKey))
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
                    if (!verifySignature(messageData.body, messageData.signature, nodeIdToPublicKey(messageData.fromNodeId))) {
                        console.warn(`Problem verifying signature on pubsub message: channel=${channelName} pubsubChannelName=${pubsubChannelName}`)
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
        const request: KacheryNodeRequest = {
            body: requestBody,
            nodeId: this.nodeId,
            signature: getSignature(requestBody, this.opts.keyPair)
        }
        const x = await axios.post(`${this._kacheryHubUrl()}/api/kacheryNode`, request)
        return x.data
    }
    _kacheryHubUrl() {
        return this.opts.kacheryHubUrl
    }
}

export default KacheryHubClient