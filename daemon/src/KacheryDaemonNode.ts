import fs from 'fs'
import { createKeyPair, getSignature, hexToPrivateKey, hexToPublicKey, privateKeyToHex, publicKeyHexToNodeId, publicKeyToHex, verifySignature } from './kachery-js/types/crypto_util'
import { ByteCount, ChannelName, FileKey, isArrayOf, isKeyPair, isString, JSONObject, JSONValue, KeyPair, LocalFilePath, NodeId, NodeLabel, Sha1Hash, UserId } from './kachery-js/types/kacheryTypes'
import { isReadableByOthers } from './common/util'
import ExternalInterface, { KacheryStorageManagerInterface } from './external/ExternalInterface'
import FeedManager from './feeds/FeedManager'
import FileUploader, { SignedFileUploadUrlCallback } from './FileUploader/FileUploader'
import { getStats, GetStatsOpts } from './getStats'
import KacheryHubInterface from './KacheryHubInterface'
import MutableManager from './mutables/MutableManager'
import NodeStats from './NodeStats'
import IncomingTaskManager from './tasks/IncomingTaskManager'
import OutgoingTaskManager from './tasks/outgoingTaskManager'

export interface KacheryDaemonNodeOpts {
    kacheryHubUrl: string
}

class KacheryDaemonNode {
    #keyPair: KeyPair
    #nodeId: NodeId
    #feedManager: FeedManager
    #mutableManager: MutableManager
    #kacheryStorageManager: KacheryStorageManagerInterface
    #stats = new NodeStats()
    #clientAuthCode = {current: '', previous: ''}
    #otherClientAuthCodes: string[] = []
    #kacheryHubInterface: KacheryHubInterface
    #fileUploader: FileUploader
    constructor(private p: {
        verbose: number,
        label: NodeLabel,
        ownerId?: UserId,
        externalInterface: ExternalInterface,
        opts: KacheryDaemonNodeOpts
    }) {
        this.#kacheryStorageManager = p.externalInterface.createKacheryStorageManager()

        this.#keyPair = this.#kacheryStorageManager.storageDir() ? _loadKeypair(this.#kacheryStorageManager.storageDir()) : createKeyPair()
        this.#nodeId = publicKeyHexToNodeId(publicKeyToHex(this.#keyPair.publicKey)) // get the node id from the public key

        const storageDir = this.#kacheryStorageManager.storageDir()

        if (storageDir) {
            fs.writeFileSync(`${storageDir}/kachery-node-id`, `${this.#nodeId}`)
        }

        this.#mutableManager = new MutableManager(storageDir)

        this._updateOtherClientAuthCodes()
        this.#mutableManager.onSet((k: JSONValue) => {
            if (k === '_other_client_auth_codes') {
                this._updateOtherClientAuthCodes()
            }
        })

        this.#kacheryHubInterface = new KacheryHubInterface({keyPair: this.#keyPair, ownerId: p.ownerId, nodeLabel: p.label, kacheryHubUrl: p.opts.kacheryHubUrl, nodeStats: this.#stats})

        this.#kacheryHubInterface.onIncomingFileRequest(({fileKey, channelName, fromNodeId}) => {
            this._handleIncomingFileRequest({fileKey, channelName, fromNodeId})
        })
        this.#kacheryHubInterface.onRequestSubfeed((channelName, feedId, subfeedHash, position) => {
            this.#feedManager.createOrRenewIncomingSubfeedSubscription(channelName, feedId, subfeedHash, position)
        })
        this.#kacheryHubInterface.onUpdateSubfeedMessageCount((channelName, feedId, subfeedHash, messageCount) => {
            this.#feedManager.reportSubfeedMessageCountUpdate(channelName, feedId, subfeedHash, messageCount)
        })

        const signedFileUploadUrlCallback: SignedFileUploadUrlCallback = async (a: {channelName: ChannelName, sha1: Sha1Hash, size: ByteCount}) => {
            return await this.#kacheryHubInterface.createSignedFileUploadUrl(a)
        }

        this.#fileUploader = new FileUploader(signedFileUploadUrlCallback, this.#kacheryStorageManager, this.#stats)

        // The feed manager -- each feed is a collection of append-only logs
        const localFeedManager = this.p.externalInterface.createLocalFeedManager(this.#mutableManager)
        this.#feedManager = new FeedManager(this.#kacheryHubInterface, localFeedManager, this.#stats)
    }
    nodeId() {
        return this.#nodeId
    }
    keyPair() {
        return this.#keyPair
    }
    kacheryStorageManager() {
        return this.#kacheryStorageManager
    }
    stats() {
        return this.#stats
    }
    cleanup() {
    }
    externalInterface() {
        return this.p.externalInterface
    }
    feedManager() {
        return this.#feedManager
    }
    mutableManager() {
        return this.#mutableManager
    }
    getStats(o: GetStatsOpts) {
        return getStats(this, o)
    }
    nodeLabel() {
        return this.p.label
    }
    ownerId() {
        return this.p.ownerId
    }
    setClientAuthCode(code: string, previousCode: string) {
        this.#clientAuthCode = {
            current: code,
            previous: previousCode
        }
    }
    verifyClientAuthCode(code: string, opts: {browserAccess: boolean}) {
        if (code === this.#clientAuthCode.current) return true
        if ((this.#clientAuthCode.previous) && (code === this.#clientAuthCode.previous)) return true
        if (!opts.browserAccess) {
            return false
        }
        if (this.#otherClientAuthCodes.includes(code)) return true
        return false
    }
    kacheryHubInterface() {
        return this.#kacheryHubInterface
    }
    async _updateOtherClientAuthCodes() {
        const x = await this.#mutableManager.get('_other_client_auth_codes')
        if (x) {
            const v = x.value
            if ((isArrayOf(isString))(v)) {
                this.#otherClientAuthCodes = v as string[]
            }
        }
    }
    async _handleIncomingFileRequest(args: {fileKey: FileKey, channelName: ChannelName, fromNodeId: NodeId}) {
        const x = await this.#kacheryStorageManager.findFile(args.fileKey)
        if (x.found) {
            this.#kacheryHubInterface.sendUploadFileStatusMessage({channelName: args.channelName, fileKey: args.fileKey, status: 'started'})
            // todo: use pending status and only upload certain number at a time
            await this.#fileUploader.uploadFileToBucket({channelName: args.channelName, fileKey: args.fileKey, fileSize: x.size})
            this.#kacheryHubInterface.sendUploadFileStatusMessage({channelName: args.channelName, fileKey: args.fileKey, status: 'finished'})
        }
    }
}


const _loadKeypair = (storageDir: LocalFilePath): KeyPair => {
    if (!fs.existsSync(storageDir.toString())) {
        /* istanbul ignore next */
        throw Error(`Storage directory does not exist: ${storageDir}`)
    }
    const publicKeyPath = `${storageDir.toString()}/public.pem`
    const privateKeyPath = `${storageDir.toString()}/private.pem`
    if (fs.existsSync(publicKeyPath)) {
        /* istanbul ignore next */
        if (!fs.existsSync(privateKeyPath)) {
            throw Error(`Public key file exists, but secret key file does not.`)
        }
    }
    else {
        const { publicKey, privateKey } = createKeyPair()
        fs.writeFileSync(publicKeyPath, publicKey.toString(), { encoding: 'utf-8' })
        fs.writeFileSync(privateKeyPath, privateKey.toString(), { encoding: 'utf-8', mode: fs.constants.S_IRUSR | fs.constants.S_IWUSR})
        fs.chmodSync(publicKeyPath, fs.constants.S_IRUSR | fs.constants.S_IWUSR)
        fs.chmodSync(privateKeyPath, fs.constants.S_IRUSR | fs.constants.S_IWUSR)
    }

    if (isReadableByOthers(privateKeyPath)) {
        throw Error(`Invalid permissions for private key file: ${privateKeyPath}`)
    }

    const keyPair = {
        publicKey: fs.readFileSync(publicKeyPath, { encoding: 'utf-8' }),
        privateKey: fs.readFileSync(privateKeyPath, { encoding: 'utf-8' }),
    }
    if (!isKeyPair(keyPair)) {
        /* istanbul ignore next */
        throw Error('Invalid keyPair')
    }
    testKeyPair(keyPair)
    return keyPair
}

const testKeyPair = (keyPair: KeyPair) => {
    const signature = getSignature({ test: 1 }, keyPair)
    if (!verifySignature({ test: 1 } as JSONObject, signature, keyPair.publicKey)) {
        /* istanbul ignore next */
        throw new Error('Problem testing public/private keys. Error verifying signature.')
    }
    if (hexToPublicKey(publicKeyToHex(keyPair.publicKey)) !== keyPair.publicKey) {
        /* istanbul ignore next */
        throw new Error('Problem testing public/private keys. Error converting public key to/from hex.')
    }
    if (hexToPrivateKey(privateKeyToHex(keyPair.privateKey)) !== keyPair.privateKey) {
        /* istanbul ignore next */
        throw new Error('Problem testing public/private keys. Error converting private key to/from hex.')
    }
}

export default KacheryDaemonNode