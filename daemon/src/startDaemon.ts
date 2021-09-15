import axios from 'axios';
import fs from 'fs';
import { createKeyPair, hexToPrivateKey, hexToPublicKey, privateKeyToHex, publicKeyToHex, signMessage, testKeyPair, verifySignature } from './commonInterface/crypto/signatures';
import ExternalInterface from './kacheryInterface/core/ExternalInterface';
import KacheryNode from './kacheryInterface/core/KacheryNode';
import { KacheryNodeRequest, KacheryNodeRequestBody } from './kacheryInterface/kacheryNodeRequestTypes';
import { isKeyPair, JSONObject, JSONValue, KeyPair, LocalFilePath, NodeLabel, Port, publicKeyHexToNodeId, Signature, UserId } from './commonInterface/kacheryTypes';
import { KacheryHubPubsubMessageBody } from './kacheryInterface/pubsubMessages';
import { isReadableByOthers } from './external/real/LocalFeedManager';
import MutableManager from './external/real/mutables/MutableManager';
import ClientAuthService from './services/ClientAuthService';
import DaemonApiServer from './services/DaemonApiServer';
import DisplayStateService from './services/DisplayStateService';
import KacheryHubService from './services/KacheryHubService';
import CleanCacheService from './services/CleanCacheService';
import { BitwooderResourceRequest, BitwooderResourceResponse } from 'bitwooderInterface/BitwooderResourceRequest';
import logger from "winston";;

export interface StartDaemonOpts {
    authGroup: string | null,
    services: {
        display?: boolean,
        daemonServer?: boolean
        mirror?: boolean,
        kacheryHub?: boolean,
        clientAuth?: boolean,
        cleanCache?: boolean
    },
    kacheryHubUrl: string,
    bitwooderUrl: string
}

export interface DaemonInterface {
    daemonApiServer: DaemonApiServer | null,
    displayService: DisplayStateService | null,
    kacheryHubService: KacheryHubService | null,
    clientAuthService: ClientAuthService | null,
    cleanCacheService: CleanCacheService | null,
    node: KacheryNode,
    stop: () => void
}

const startDaemon = async (args: {
    verbose: number,
    daemonApiPort: Port | null,
    label: NodeLabel,
    ownerId?: UserId,
    externalInterface: ExternalInterface,
    opts: StartDaemonOpts
}): Promise<DaemonInterface> => {
    const {
        verbose,
        daemonApiPort,
        label,
        ownerId,
        externalInterface,
        opts
    } = args

    const kacheryStorageManager = externalInterface.createKacheryStorageManager()


    const storageDir = kacheryStorageManager.storageDir()
    const keyPair = storageDir ? await _loadKeypair(storageDir) : await createKeyPair()
    const nodeId = publicKeyHexToNodeId(publicKeyToHex(keyPair.publicKey)) // get the node id from the public key

    if (storageDir) {
        fs.writeFileSync(`${storageDir}/kachery-node-id`, `${nodeId}`)
    }
    const mutableManager = new MutableManager(storageDir)
    const localFeedManager = externalInterface.createLocalFeedManager(mutableManager)

    const sendKacheryNodeRequest = async (requestBody: KacheryNodeRequestBody): Promise<JSONValue> => {
        const request: KacheryNodeRequest = {
            body: requestBody,
            nodeId,
            signature: await signMessage(requestBody as any as JSONValue, keyPair)
        }
        const x = await axios.post(`${opts.kacheryHubUrl}/api/kacheryNode`, request)
        return x.data
    }
    const sendBitwooderResourceRequest = async (request: BitwooderResourceRequest): Promise<BitwooderResourceResponse> => {
        const x = await axios.post(`${opts.bitwooderUrl}/api/resource`, request)
        return x.data as any as BitwooderResourceResponse
    }
    const signPubsubMessage2 = async (messageBody: KacheryHubPubsubMessageBody): Promise<Signature> => {
        return await signMessage(messageBody as any as JSONValue, keyPair)
    }

    const kNode = new KacheryNode({
        verbose,
        nodeId,
        sendKacheryNodeRequest,
        sendBitwooderResourceRequest,
        signPubsubMessage: signPubsubMessage2,
        label,
        ownerId,
        kacheryStorageManager,
        mutableManager,
        localFeedManager,
        opts: {
            kacheryHubUrl: opts.kacheryHubUrl,
            bitwooderUrl: opts.bitwooderUrl,
            verifySubfeedMessageSignatures: true
        }
    })

    // Start the daemon http server
    const daemonApiServer = new DaemonApiServer(kNode, externalInterface, { verbose });
    if (opts.services.daemonServer && (daemonApiPort !== null)) {
        await daemonApiServer.listen(daemonApiPort);
        logger.info(`Daemon http server listening on port ${daemonApiPort}`)
    }

    // start the other services
    let displayService = opts.services.display ? new DisplayStateService(kNode, {
        daemonApiPort
    }) : null
    const kacheryHubService = opts.services.kacheryHub ? new KacheryHubService(kNode, {
    }): null
    const clientAuthService = opts.services.clientAuth ? new ClientAuthService(kNode, {
        clientAuthGroup: opts.authGroup ? opts.authGroup : null
    }) : null
    const cleanCacheService = opts.services.cleanCache ? new CleanCacheService(kNode, {
    }) : null

    const _stop = () => {
        displayService && displayService.stop()
        kacheryHubService && kacheryHubService.stop()
        clientAuthService && clientAuthService.stop()
        cleanCacheService && cleanCacheService.stop()
        // wait a bit after stopping services before cleaning up the rest (for clean exit of services)
        setTimeout(() => {
            daemonApiServer && daemonApiServer.stop()
            setTimeout(() => {
                kNode.cleanup()
            }, 20)
        }, 20)
    }

    return {
        daemonApiServer,
        displayService,
        kacheryHubService,
        clientAuthService,
        cleanCacheService,
        node: kNode,
        stop: _stop
    }
}

const _loadKeypair = async (storageDir: LocalFilePath): Promise<KeyPair> => {
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
        const { publicKey, privateKey } = await createKeyPair()
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
    await testKeyPair(keyPair)
    await testKeyPair2(keyPair)
    return keyPair
}

const testKeyPair2 = async (keyPair: KeyPair) => {
    const signature = await signMessage({ test: 1 }, keyPair)
    if (!await verifySignature({ test: 1 } as JSONObject, keyPair.publicKey, signature)) {
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

export default startDaemon