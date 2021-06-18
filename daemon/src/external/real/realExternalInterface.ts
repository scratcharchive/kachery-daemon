import dgram from 'dgram';
import { LocalFilePath } from 'kachery-js/types/kacheryTypes';
import MutableManager from './mutables/MutableManager';
import ExternalInterface, { LocalFeedManagerInterface, MutableManagerInterface } from 'kachery-js/core/ExternalInterface';
import { httpGetDownload, httpPostJson } from "./httpRequests";
import { KacheryStorageManager } from './kacheryStorage/KacheryStorageManager';
import LocalFeedManager from './LocalFeedManager';
import startHttpServer from './startHttpServer';
import { createWebSocket, startWebSocketServer } from './webSocket';

const realExternalInterface = (storageDir: LocalFilePath): ExternalInterface => {
    const dgramCreateSocket = (args: { type: 'udp4', reuseAddr: boolean }) => {
        return dgram.createSocket({ type: args.type, reuseAddr: args.reuseAddr })
    }

    const createKacheryStorageManager = () => {
        return new KacheryStorageManager(storageDir)
    }

    const createLocalFeedManager = (mutableManager: MutableManagerInterface): LocalFeedManagerInterface => {
        return new LocalFeedManager(storageDir, mutableManager)
    }

    const createMutableManager = (): MutableManagerInterface => {
        return new MutableManager(storageDir)
    }

    return {
        httpPostJson,
        httpGetDownload,
        dgramCreateSocket,
        startWebSocketServer,
        createWebSocket,
        createKacheryStorageManager,
        createMutableManager,
        createLocalFeedManager,
        startHttpServer,
        isMock: false
    }
}

export default realExternalInterface