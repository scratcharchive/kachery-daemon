import axios from "axios";
import { getSignature } from "../common/types/crypto_util";
import { NodeConfig } from "../common/types/kacheryHubTypes";
import { ByteCount, elapsedSince, FeedId, FileKey, fileKeyHash, isJSONObject, isMessageCount, isSignedSubfeedMessage, JSONValue, KeyPair, maxDuration, MessageCount, NodeId, NodeLabel, nowTimestamp, Sha1Hash, SignedSubfeedMessage, SubfeedHash, SubfeedPosition, urlString, UrlString, _validateObject } from "../common/types/kacheryTypes";
import { KacheryHubPubsubMessageBody, KacheryHubPubsubMessageData, SubfeedMessageCountUpdateMessageBody, RequestSubfeedMessageBody, UploadFileStatusMessageBody, RequestFileMessageBody } from "../common/types/pubsubMessages";
import { urlFromUri } from '../common/util';
import GoogleObjectStorageClient from "./GoogleObjectStorageClient";
import KacheryHubClient, { IncomingKacheryHubPubsubMessage } from "./KacheryHubClient";

type IncomingFileRequestCallback = (args: {fileKey: FileKey, fromNodeId: NodeId, channelName: string}) => void

class KacheryHubInterface {
    #kacheryHubClient: KacheryHubClient
    #nodeConfig: NodeConfig | null = null
    #initialized = false
    #initializing = false
    #onInitializedCallbacks: (() => void)[] = []
    #incomingFileRequestCallbacks: IncomingFileRequestCallback[] = []
    #requestSubfeedCallbacks: ((channelName: string, feedId: FeedId, subfeedHash: SubfeedHash, position: SubfeedPosition) => void)[] = []
    #subfeedMessageCountUpdateCallbacks: ((feedId: FeedId, subfeedHash: SubfeedHash, channelName: string, messageCount: MessageCount) => void)[] = []
    constructor(private opts: {keyPair: KeyPair, ownerId: string, nodeLabel: NodeLabel, kacheryHubUrl: string}) {
        const {keyPair, ownerId, nodeLabel, kacheryHubUrl} = opts
        this.#kacheryHubClient = new KacheryHubClient({keyPair, ownerId, nodeLabel, kacheryHubUrl})
        this.#kacheryHubClient.onIncomingPubsubMessage((x: IncomingKacheryHubPubsubMessage) => {
            this._handleKacheryHubPubsubMessage(x)
        })
        this.initialize()
    }
    client() {
        return this.#kacheryHubClient
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
                    const msg: RequestFileMessageBody = {
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
    onIncomingPubsubMessage(cb: (x: IncomingKacheryHubPubsubMessage) => void) {
        return this.#kacheryHubClient.onIncomingPubsubMessage(cb)
    }
    onIncomingFileRequest(callback: IncomingFileRequestCallback) {
        this.#incomingFileRequestCallbacks.push(callback)
    }
    onRequestSubfeed(cb: (channelName: string, feedId: FeedId, subfeedHash: SubfeedHash, position: SubfeedPosition) => void) {
        this.#requestSubfeedCallbacks.push(cb)
    }
    onSubfeedMessageCountUpdate(callback: (feedId: FeedId, subfeedHash: SubfeedHash, channelName: string, messageCount: MessageCount) => void) {
        this.#subfeedMessageCountUpdateCallbacks.push(callback)
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
    async createSignedFileUploadUrl(a: {channelName: string, sha1: Sha1Hash, size: ByteCount}) {
        return this.#kacheryHubClient.createSignedFileUploadUrl(a)
    }
    async createSignedSubfeedMessageUploadUrls(a: {channelName: string, feedId: FeedId, subfeedHash: SubfeedHash, messageNumberRange: [number, number]}) {
        return this.#kacheryHubClient.createSignedSubfeedMessageUploadUrls(a)
    }
    async reportToChannelSubfeedMessagesAdded(channelName: string, feedId: FeedId, subfeedHash: SubfeedHash, numMessages: MessageCount) {
        await this.initialize()
        const msg: SubfeedMessageCountUpdateMessageBody = {
            type: 'subfeedMessageCountUpdate',
            feedId,
            subfeedHash,
            messageCount: numMessages
        }
        this._publishMessageToPubsubChannel(channelName, `${channelName}-provideFeeds`, msg)
    }
    async subscribeToRemoteSubfeed(feedId: FeedId, subfeedHash: SubfeedHash, position: SubfeedPosition) {
        await this.initialize()
        const nodeConfig = this.#nodeConfig
        if (!nodeConfig) return
        const channelNames: string[] = []
        for (let channelMembership of (nodeConfig.channelMemberships || [])) {
            if (channelMembership.roles.requestFeeds) {
                if ((channelMembership.authorization) && (channelMembership.authorization.permissions.requestFeeds)) {
                    channelNames.push(channelMembership.channelName)
                }
            }
        }
        const msg: RequestSubfeedMessageBody = {
            type: 'requestSubfeed',
            feedId,
            subfeedHash,
            position
        }
        for (let channelName of channelNames) {
            this._publishMessageToPubsubChannel(channelName, `${channelName}-requestFeeds`, msg)
        }
    }
    async downloadSignedSubfeedMessages(channelName: string, feedId: FeedId, subfeedHash: SubfeedHash, start: MessageCount, end: MessageCount): Promise<SignedSubfeedMessage[]> {
        await this.initialize()
        const nodeConfig = this.#nodeConfig
        if (!nodeConfig) {
            throw Error('Problem initializing kacheryhub interface')
        }
        const channelMembership = (nodeConfig.channelMemberships || []).filter(cm => (cm.channelName === channelName))[0]
        if (!channelMembership) {
            throw Error(`Not a member of channel: ${channelName}`)
        }
        const channelBucketUri = channelMembership.channelBucketUri
        if (!channelBucketUri) {
            throw Error(`No bucket uri for channel: ${channelName}`)
        }
        const channelBucketName = bucketNameFromUri(channelBucketUri)
        
        const subfeedJson = await this.loadSubfeedJson(channelName, feedId, subfeedHash)
        if (!subfeedJson) {
            throw Error(`Unable to load subfeed.json for subfeed: ${feedId} ${subfeedHash} ${channelName}`)
        }
        if (Number(subfeedJson.messageCount) < Number(end)) {
            throw Error(`Not enough messages for subfeed: ${feedId} ${subfeedHash} ${channelName}`)
        }
        const subfeedPath = getSubfeedPath(feedId, subfeedHash)

        const client = new GoogleObjectStorageClient({bucketName: channelBucketName})

        const ret: SignedSubfeedMessage[] = []
        for (let i = Number(start); i < Number(end); i++) {
            const messagePath = `${subfeedPath}/${i}`
            const messageJson = await client.getObjectJson(messagePath, {cacheBust: false})
            if (!messageJson) {
                throw Error(`Unable to download subfeed message ${messagePath} on ${channelBucketName}`)
            }
            if (!isSignedSubfeedMessage(messageJson)) {
                throw Error(`Invalid subfeed message ${messagePath} on ${channelBucketName}`)
            }
            ret.push(messageJson)
        }
        return ret
    }
    async getChannelBucketName(channelName: string) {
        await this.initialize()
        const channelMembership = this._getChannelMembership(channelName)
        if (!channelMembership) throw Error(`Not a member of channel: ${channelName}`)
        const channelBucketUri = channelMembership.channelBucketUri
        if (!channelBucketUri) {
            throw Error(`No bucket uri for channel: ${channelName}`)
        }
        const channelBucketName = bucketNameFromUri(channelBucketUri)
        return channelBucketName
    }
    async loadSubfeedJson(channelName: string, feedId: FeedId, subfeedHash: SubfeedHash) {
        const channelBucketName = await this.getChannelBucketName(channelName)
        const subfeedPath = getSubfeedPath(feedId, subfeedHash)
        const subfeedJsonPath = `${subfeedPath}/subfeed.json`
        const client = new GoogleObjectStorageClient({bucketName: channelBucketName})
        const subfeedJson = await client.getObjectJson(subfeedJsonPath, {cacheBust: true})
        if (!subfeedJson) {
            return null
        }
        if (!isSubfeedJson(subfeedJson)) {
            throw Error(`Problem with subfeed.json for ${subfeedPath} on ${channelBucketName}`)
        }
        return subfeedJson
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
                cb({fileKey: msg.fileKey, channelName: x.channelName, fromNodeId: x.fromNodeId})
            })
        }
        else if (msg.type === 'requestSubfeed') {
            if (x.pubsubChannelName !== `${x.channelName}-requestFeeds`) {
                console.warn(`Unexpected pubsub channel for requestSubfeed: ${x.pubsubChannelName}`)
                return
            }
            const nodeConfig = this.#nodeConfig
            if (!nodeConfig) return
            const {channelName} = x
            const channelMembership = (nodeConfig.channelMemberships || []).filter(cm => (cm.channelName === channelName))[0]
            if (!channelMembership) return
            if ((channelMembership.roles.provideFeeds) && (channelMembership.authorization) && (channelMembership.authorization.permissions.provideFeeds)) {
                this.#requestSubfeedCallbacks.forEach(cb => {
                    cb(channelName, msg.feedId, msg.subfeedHash, msg.position)
                })
            }
        }
        else if (msg.type === 'subfeedMessageCountUpdate') {
            if (x.pubsubChannelName !== `${x.channelName}-provideFeeds`) {
                console.warn(`Unexpected pubsub channel for subfeedMessageCountUpdate: ${x.pubsubChannelName}`)
                return
            }
            this.#subfeedMessageCountUpdateCallbacks.forEach(cb => {
                cb(msg.feedId, msg.subfeedHash, x.channelName, msg.messageCount)
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
                if ((au.permissions.requestFeeds) && (cm.roles.requestFeeds)) {
                    // if we are requesting feeds, then we need to listen to provideFeeds channel
                    subscribeToPubsubChannels.push(`${cm.channelName}-provideFeeds`)
                }
                if ((au.permissions.provideFeeds) && (cm.roles.provideFeeds)) {
                    // if we are providing feeds, then we need to listen to requestFeeds channel
                    subscribeToPubsubChannels.push(`${cm.channelName}-requestFeeds`)
                }
                // todo: think about how to handle case where authorization has changed, and so we need to subscribe to different pubsub channels
                // for now, the channel is not recreated
                this.#kacheryHubClient.createPubsubClientForChannel(cm.channelName, subscribeToPubsubChannels)
            }
        }
        this.#nodeConfig = nodeConfig
    }
}

const getSubfeedPath = (feedId: FeedId, subfeedHash: SubfeedHash) => {
    const f = feedId.toString()
    const s = subfeedHash.toString()
    const subfeedPath = `feeds/{f[0]}{f[1]}/{f[2]}{f[3]}/{f[4]}{f[5]}/{f}/subfeeds/{s[0]}{s[1]}/{s[2]}{s[3]}//{s[4]}{s[5]}/{s}`
    return subfeedPath
}

type SubfeedJson = {
    messageCount: MessageCount
}
const isSubfeedJson = (x: any): x is SubfeedJson => {
    return _validateObject(x, {
        messageCount: isMessageCount
    }, {allowAdditionalFields: true})
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

const bucketNameFromUri = (bucketUri: string) => {
    if (!bucketUri.startsWith('gs://')) throw Error(`Invalid bucket uri: ${bucketUri}`)
    const a = bucketUri.split('/')
    return a[2]
}

export default KacheryHubInterface