import axios from "axios"
import { getSignature, nodeIdToPublicKey, publicKeyHexToNodeId, publicKeyToHex, verifySignature } from "../common/types/crypto_util"
import { isNodeConfig, isPubsubAuth, PubsubAuth } from "../common/types/kacheryHubTypes"
import { CreateSignedUploadUrlRequestBody, GetNodeConfigRequestBody, GetPubsubAuthForChannelRequestBody, isCreateSignedUploadUrlResponse, KacheryNodeRequest, KacheryNodeRequestBody, ReportRequestBody } from "../common/types/kacheryNodeRequestTypes"
import { ByteCount, JSONValue, KeyPair, NodeId, NodeLabel, Sha1Hash } from "../common/types/kacheryTypes"
import createPubsubClient, { PubsubClient, PubsubMessage } from "./createPubsubClient"
import {isKacheryHubPubsubMessageData, KacheryHubPubsubMessageBody, KacheryHubPubsubMessageData} from '../common/types/pubsubMessages'
import { AblyAuthCallback, AblyAuthCallbackCallback } from "./AblyPubsubClient"
import Ably from 'ably'
import { randomAlphaString } from "../common/util"

export type IncomingKacheryHubPubsubMessage = {
    channelName: string,
    pubsubChannelName: string,
    fromNodeId: NodeId,
    message: KacheryHubPubsubMessageBody
}

class KacheryHubClient {
    #pubsubClients: {[key: string]: PubsubClient} = {}
    #incomingPubsubMessageCallbacks: {[key: string]: (x: IncomingKacheryHubPubsubMessage) => void} = {}
    constructor(private opts: {keyPair: KeyPair, ownerId: string, nodeLabel: NodeLabel, kacheryHubUrl?: string}) {
    }
    async fetchNodeConfig() {
        const reqBody: GetNodeConfigRequestBody = {
            type: 'getNodeConfig',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId
        }
        const nodeConfig = await this._sendRequest(reqBody)
        if (!isNodeConfig(nodeConfig)) {
            console.warn(nodeConfig)
            throw Error('Invalid node config')
        }
        return nodeConfig
    }
    async fetchPubsubAuthForChannel(channelName: string) {
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
        const reqBody: ReportRequestBody = {
            type: 'report',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId,
            nodeLabel: this.opts.nodeLabel
        }
        await this._sendRequest(reqBody)
    }
    async createSignedUploadUrl(bucketUri: string, sha1: Sha1Hash, size: ByteCount) {
        const reqBody: CreateSignedUploadUrlRequestBody = {
            type: 'createSignedUploadUrl',
            nodeId: this.nodeId,
            ownerId: this.opts.ownerId,
            bucketUri,
            sha1,
            size
        }
        const x = await this._sendRequest(reqBody)
        if (!isCreateSignedUploadUrlResponse(x)) {
            throw Error('Unexpected response for createSignedUploadUrl')
        }
        return x.signedUrl
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
    createPubsubClientForChannel(channelName: string, subscribeToPubsubChannels: string[]) {
        if (channelName in this.#pubsubClients) {
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
        this.#pubsubClients[channelName] = client
    }
    getPubsubClientForChannel(channelName: string) {
        if (channelName in this.#pubsubClients) {
            return this.#pubsubClients[channelName]
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
        return this.opts.kacheryHubUrl || 'https://kachery-hub.vercel.app'
    }
}

export default KacheryHubClient