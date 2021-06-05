import axios from "axios";
import { getSignature } from "../common/types/crypto_util";
import { NodeConfig } from "../common/types/kacheryHubTypes";
import { ByteCount, elapsedSince, FileKey, fileKeyHash, JSONValue, KeyPair, NodeId, NodeLabel, nowTimestamp, Sha1Hash, Timestamp, urlString, UrlString, zeroTimestamp } from "../common/types/kacheryTypes";
import { KacheryHubPubsubMessageBody, KacheryHubPubsubMessageData, RequestFilePubsubMessageBody, UploadFileStatusMessageBody } from "../common/types/pubsubMessages";
import { urlFromUri } from '../common/util';
import KacheryHubClient, { IncomingKacheryHubPubsubMessage } from "./KacheryHubClient";

type IncomingFileRequestCallback = (args: {fileKey: FileKey, fromNodeId: NodeId, channelName: string, bucketUri: string}) => void

class KacheryHubInterface {
    #kacheryHubClient: KacheryHubClient
    #nodeConfig: NodeConfig | null = null
    #initialized = false
    #initializing = false
    #onInitializedCallbacks: (() => void)[] = []
    #incomingFileRequestCallbacks: IncomingFileRequestCallback[] = []
    constructor(private opts: {keyPair: KeyPair, ownerId: string, nodeLabel: NodeLabel, kacheryHubUrl?: string}) {
        const {keyPair, ownerId, nodeLabel, kacheryHubUrl} = opts
        this.#kacheryHubClient = new KacheryHubClient({keyPair, ownerId, nodeLabel, kacheryHubUrl})
        this.#kacheryHubClient.onIncomingPubsubMessage((x: IncomingKacheryHubPubsubMessage) => {
            this._handleKacheryHubPubsubMessage(x)
        })
        this.initialize()
    }
    async initialize() {
        if (this.#initialized) return
        if (this.#initializing) {
            return new Promise<void>((resolve) => {
                this.#onInitializedCallbacks.push(() => {
                    resolve()
                })
            })
        }
        this.#initializing = true
        await this._doInitialize()
        this.#initialized = true
        this.#initializing = false
        this.#onInitializedCallbacks.forEach(cb => {cb()})
    }
    async checkForFileInChannelBuckets(sha1: Sha1Hash): Promise<UrlString[] | null> {
        await this.initialize()
        const nodeConfig = this.#nodeConfig
        if (!nodeConfig) return null
        const bucketUris: string[] = (nodeConfig.channelMemberships || []).map(cm => (cm.channelBucketUri)).filter(uri => (uri !== undefined)).map(uri => (uri as string))
        const uniqueBucketUris = [...new Set(bucketUris)]
        const uniqueBucketUrls = uniqueBucketUris.map(uri => urlFromUri(uri)).map(url => (urlString(url)))
        const ret: UrlString[] = []
        for (let url of uniqueBucketUrls) {
            const s = sha1.toString()
            const url2 = urlString(`${url}/sha1/${s[0]}${s[1]}/${s[2]}${s[3]}/${s[4]}${s[5]}/${s}`)
            if (await checkUrlExists(url2)) {
                ret.push(url2)
            }
        }
        return ret
    }
    async requestFileFromChannels(fileKey: FileKey): Promise<boolean> {
        await this.initialize()
        const nodeConfig = this.#nodeConfig
        if (!nodeConfig) return false
        let status: '' | 'pending' | 'started' | 'finished' = ''
        let stageFromStatus: {[key: string]: number} = {
            '': 0,
            'pending': 1,
            'started': 2,
            'finished': 3
        }
        return new Promise<boolean>((resolve) => {
            let timer = nowTimestamp()
            let complete = false
            const {cancel: cancelListener} = this.#kacheryHubClient.onIncomingPubsubMessage((msg) => {
                if (complete) return
                if ((msg.message.type === 'uploadFileStatus') && (fileKeysMatch(msg.message.fileKey, fileKey))) {
                    const newStatus = msg.message.status
                    const currentStage = stageFromStatus[status]
                    const newStage = stageFromStatus[newStatus]
                    if (newStage > currentStage) {
                        status = newStatus
                        timer = nowTimestamp()
                    }
                    if (status === 'finished') {
                        complete = true
                        cancelListener()
                        resolve(true)
                    }
                }
            })
            for (let cm of (nodeConfig.channelMemberships || [])) {
                const au = cm.authorization
                if ((au) && (au.permissions.requestFiles)) {
                    const msg: RequestFilePubsubMessageBody = {
                        type: 'requestFile',
                        fileKey
                    }
                    this._publishMessageToPubsubChannel(cm.channelName, `${cm.channelName}-requestFiles`, msg)
                }
            }
            const check = () => {
                if (complete) return
                const _finalize = () => {
                    complete = true
                    cancelListener()
                    resolve(false)
                }
                const elapsed = elapsedSince(timer)
                if (status === '') {
                    if (elapsed > 3000) {
                        _finalize()
                        return
                    }
                }
                else if (status === 'pending') {
                    if (elapsed > 30000) {
                        _finalize()
                        return
                    }
                }
                else if (status === 'started') {
                    if (elapsed > 30000) {
                        _finalize()
                        return
                    }
                }
                setTimeout(check, 1001)
            }
            check()
        })
    }
    onIncomingFileRequest(callback: IncomingFileRequestCallback) {
        this.#incomingFileRequestCallbacks.push(callback)
    }
    async sendUploadFileStatusMessage(args: {channelName: string, fileKey: FileKey, status: 'started' | 'finished'}) {
        const {channelName, fileKey, status} = args
        await this.initialize()
        const msg: UploadFileStatusMessageBody = {
            type: 'uploadFileStatus',
            fileKey,
            status
        }
        this._publishMessageToPubsubChannel(channelName, `${channelName}-provideFiles`, msg)
    }
    async getNodeConfig() {
        await this.initialize()
        return this.#nodeConfig
    }
    async createSignedUploadUrl(bucketUri: string, sha1: Sha1Hash, size: ByteCount) {
        return this.#kacheryHubClient.createSignedUploadUrl(bucketUri, sha1, size)
    }
    _getChannelMembership(channelName: string) {
        if (!this.#nodeConfig) return
        const x = (this.#nodeConfig.channelMemberships || []).filter(cm => (cm.channelName === channelName))[0]
        if (!x) return undefined
        return x
    }
    _publishMessageToPubsubChannel(channelName: string, pubsubChannelName: string, messageBody: KacheryHubPubsubMessageBody) {
        const pubsubClient = this.#kacheryHubClient.getPubsubClientForChannel(channelName)
        if (pubsubClient) {
            const pubsubChannel = pubsubClient.getChannel(pubsubChannelName)
            const m: KacheryHubPubsubMessageData = {
                body: messageBody,
                fromNodeId: this.#kacheryHubClient.nodeId,
                signature: getSignature(messageBody, this.opts.keyPair)
            }
            pubsubChannel.publish({data: m as any as JSONValue})    
        }
    }
    _handleKacheryHubPubsubMessage(x: IncomingKacheryHubPubsubMessage) {
        const msg = x.message
        if (msg.type === 'requestFile') {
            if (x.pubsubChannelName !== `${x.channelName}-requestFiles`) {
                console.warn(`Unexpected pubsub channel for requestFile: ${x.pubsubChannelName}`)
                return
            }
            const cm = this._getChannelMembership(x.channelName)
            if (!cm) return
            const bucketUri = cm.channelBucketUri
            if (!bucketUri) return
            this.#incomingFileRequestCallbacks.forEach(cb => {
                cb({fileKey: msg.fileKey, channelName: x.channelName, fromNodeId: x.fromNodeId, bucketUri})
            })
        }
    }
    async _doInitialize() {
        let nodeConfig: NodeConfig
        try {
            nodeConfig = await this.#kacheryHubClient.fetchNodeConfig()
        }
        catch(err) {
            console.warn('Problem fetching node config.', err.message)
            return
        }
        // initialize the pubsub clients so we can subscribe to the pubsub channels
        for (let cm of (nodeConfig.channelMemberships || [])) {
            const au = cm.authorization
            if (au) {
                const subscribeToPubsubChannels: string[] = []
                if ((au.permissions.requestFiles) && (cm.roles.requestFiles)) {
                    // if we are requesting files, then we need to listen to provideFiles channel
                    subscribeToPubsubChannels.push(`${cm.channelName}-provideFiles`)
                }
                if ((au.permissions.provideFiles) && (cm.roles.provideFiles)) {
                    // if we are providing files, then we need to listen to requestFiles channel
                    subscribeToPubsubChannels.push(`${cm.channelName}-requestFiles`)
                }
                // todo: think about how to handle case where authorization has changed, and so we need to subscribe to different pubsub channels
                // for now, the channel is not recreated
                this.#kacheryHubClient.createPubsubClientForChannel(cm.channelName, subscribeToPubsubChannels)
            }
        }
        this.#nodeConfig = nodeConfig
    }
}

const fileKeysMatch = (fileKey1: FileKey, fileKey2: FileKey) => {
    return fileKeyHash(fileKey1) === fileKeyHash(fileKey2)
}

const checkUrlExists = async (url: UrlString) => {
    try {
        const res = await axios.head(url.toString())
        return (res.status === 200)
    }
    catch(err) {
        return false
    }
}

export default KacheryHubInterface