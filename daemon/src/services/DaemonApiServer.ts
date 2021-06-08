import cors from 'cors';
import express, { Express, Request, Response } from 'express';
import JsonSocket from 'json-socket';
import { Socket } from 'net';
import { action } from '../common/action';
import DataStreamy from '../common/DataStreamy';
import { byteCount, ByteCount, DaemonVersion, DurationMsec, durationMsecToNumber, elapsedSince, ErrorMessage, FeedId, FeedName, FileKey, isArrayOf, isBoolean, isByteCount, isDaemonVersion, isDurationMsec, isFeedId, isFeedName, isFileKey, isJSONObject, isJSONValue, isMessageCount, isNodeId, isNull, isNumber, isObjectOf, isOneOf, isSignedSubfeedMessage, isString, isSubfeedHash, isSubfeedMessage, isSubfeedPosition, isSubfeedWatches, isSubmittedSubfeedMessage, JSONObject, JSONValue, LocalFilePath, mapToObject, messageCount, MessageCount, NodeId, nowTimestamp, optional, Port, scaledDurationMsec, Sha1Hash, SignedSubfeedMessage, SubfeedHash, SubfeedMessage, SubfeedPosition, SubfeedWatches, SubmittedSubfeedMessage, toSubfeedWatchesRAM, _validateObject } from '../common/types/kacheryTypes';
import { sleepMsec } from '../common/util';
import daemonVersion from '../daemonVersion';
import { HttpServerInterface } from '../external/ExternalInterface';
import { isGetStatsOpts, NodeStatsInterface } from '../getStats';
import KacheryDaemonNode from '../KacheryDaemonNode';
import { loadFile } from '../loadFile';

export interface DaemonApiProbeResponse {
    success: boolean,
    daemonVersion: DaemonVersion,
    nodeId: NodeId,
    kacheryStorageDir: LocalFilePath | null
};
export const isDaemonApiProbeResponse = (x: any): x is DaemonApiProbeResponse => {
    return _validateObject(x, {
        success: isBoolean,
        daemonVersion: isDaemonVersion,
        nodeId: isNodeId,
        kacheryStorageDir: isOneOf([isNull, isString])
    }, {allowAdditionalFields: true});
}

type StoreFileRequestData = {
    localFilePath: LocalFilePath
}
const isStoreFileRequestData = (x: any): x is StoreFileRequestData => {
    return _validateObject(x, {
        localFilePath: isString
    })
}
type StoreFileResponseData = {
    success: boolean
    error: ErrorMessage | null
    sha1: Sha1Hash | null
    manifestSha1: Sha1Hash | null
}

type LinkFileRequestData = {
    localFilePath: LocalFilePath
    size: number
    mtime: number
}
const isLinkFileRequestData = (x: any): x is LinkFileRequestData => {
    return _validateObject(x, {
        localFilePath: isString,
        size: isNumber,
        mtime: isNumber
    })
}
type LinkFileResponseData = {
    success: boolean
    error: ErrorMessage | null
    sha1: Sha1Hash | null
    manifestSha1: Sha1Hash | null
}

// interface Req {
//     body: any,
//     on: (eventName: string, callback: () => void) => void,
//     connection: Socket
// }

// interface Res {
//     json: (obj: {
//         success: boolean
//     } & JSONObject) => void,
//     end: () => void,
//     status: (s: number) => Res,
//     send: (x: any) => Res
// }

export interface ApiFindFileRequest {
    fileKey: FileKey,
    timeoutMsec: DurationMsec
}
const isApiFindFileRequest = (x: any): x is ApiFindFileRequest => {
    return _validateObject(x, {
        fileKey: isFileKey,
        timeoutMsec: isDurationMsec
    });
}

export interface ApiLoadFileRequest {
    fileKey: FileKey
}
const isApiLoadFileRequest = (x: any): x is ApiLoadFileRequest => {
    return _validateObject(x, {
        fileKey: isFileKey
    });
}

export interface ApiDownloadFileDataRequest {
    fileKey: FileKey,
    startByte?: ByteCount
    endByte?: ByteCount
}
const isApiDownloadFileDataRequest = (x: any): x is ApiDownloadFileDataRequest => {
    return _validateObject(x, {
        fileKey: isFileKey,
        startByte: optional(isByteCount),
        endByte: optional(isByteCount),
    });
}

export interface FeedApiWatchForNewMessagesRequest {
    subfeedWatches: SubfeedWatches,
    waitMsec: DurationMsec,
    maxNumMessages?: MessageCount,
    signed?: boolean
}
export const isFeedApiWatchForNewMessagesRequest = (x: any): x is FeedApiWatchForNewMessagesRequest => {
    return _validateObject(x, {
        subfeedWatches: isSubfeedWatches,
        waitMsec: isDurationMsec,
        signed: optional(isBoolean),
        maxNumMessages: optional(isMessageCount)
    })
}
export interface FeedApiWatchForNewMessagesResponse {
    success: boolean,
    messages: {[key: string]: SubfeedMessage[]} | {[key: string]: SignedSubfeedMessage[]}
}
export const isFeedApiWatchForNewMessagesResponse = (x: any): x is FeedApiWatchForNewMessagesResponse => {
    return _validateObject(x, {
        success: isBoolean,
        messages: isOneOf([isObjectOf(isString, isArrayOf(isSubfeedMessage)), isObjectOf(isString, isArrayOf(isSignedSubfeedMessage))])
    })
}

export interface MutableApiSetRequest {
    key: JSONValue
    value: JSONValue
}
export const isMutableApiSetRequest = (x: any): x is MutableApiSetRequest => {
    return _validateObject(x, {
        key: isJSONValue,
        value: isJSONValue
    })
}
export interface MutableApiSetResponse {
    success: boolean
}
export const isMutableApiSetResponse = (x: any): x is MutableApiSetResponse => {
    return _validateObject(x, {
        success: isBoolean
    })
}

export interface MutableApiGetRequest {
    key: JSONValue
}
export const isMutableApiGetRequest = (x: any): x is MutableApiGetRequest => {
    return _validateObject(x, {
        key: isJSONValue
    })
}
export interface MutableApiGetResponse {
    success: boolean,
    found: boolean,
    value: JSONValue
}
export const isMutableApiGetResponse = (x: any): x is MutableApiGetResponse => {
    return _validateObject(x, {
        success: isBoolean,
        found: isBoolean,
        value: isJSONValue
    })
}

export interface MutableApiDeleteRequest {
    key: JSONValue
}
export const isMutableApiDeleteRequest = (x: any): x is MutableApiDeleteRequest => {
    return _validateObject(x, {
        key: isJSONValue
    })
}
export interface MutableApiDeleteResponse {
    success: boolean
}
export const isMutableApiDeleteResponse = (x: any): x is MutableApiDeleteResponse => {
    return _validateObject(x, {
        success: isBoolean
    })
}

export interface FeedApiGetMessagesRequest {
    feedId: FeedId,
    subfeedHash: SubfeedHash,
    position: SubfeedPosition,
    maxNumMessages: MessageCount,
    waitMsec: DurationMsec
}
export const isFeedApiGetMessagesRequest = (x: any): x is FeedApiGetMessagesRequest => {
    return _validateObject(x, {
        feedId: isFeedId,
        subfeedHash: isSubfeedHash,
        position: isSubfeedPosition,
        maxNumMessages: isMessageCount,
        waitMsec: isDurationMsec,
    });
}
export interface FeedApiGetMessagesResponse {
    success: boolean,
    messages: SubfeedMessage[]
}
export const isFeedApiGetMessagesResponse = (x: any): x is FeedApiGetMessagesResponse => {
    return _validateObject(x, {
        success: isBoolean,
        messages: isArrayOf(isSubfeedMessage)
    });
}

export interface FeedApiGetSignedMessagesRequest {
    feedId: FeedId,
    subfeedHash: SubfeedHash,
    position: SubfeedPosition,
    maxNumMessages: MessageCount,
    waitMsec: DurationMsec
}
export const isFeedApiGetSignedMessagesRequest = (x: any): x is FeedApiGetSignedMessagesRequest => {
    return _validateObject(x, {
        feedId: isFeedId,
        subfeedHash: isSubfeedHash,
        position: isSubfeedPosition,
        maxNumMessages: isMessageCount,
        waitMsec: isDurationMsec,
    });
}
export interface FeedApiGetSignedMessagesResponse {
    success: boolean,
    signedMessages: SignedSubfeedMessage[]
}
export const isFeedApiGetSignedMessagesResponse = (x: any): x is FeedApiGetSignedMessagesResponse => {
    return _validateObject(x, {
        success: isBoolean,
        signedMessages: isArrayOf(isSignedSubfeedMessage)
    });
}

export interface FeedApiCreateFeedRequest {
    feedName?: FeedName
}
export const isFeedApiCreateFeedRequest = (x: any): x is FeedApiCreateFeedRequest => {
    return _validateObject(x, {
        feedName: optional(isFeedName)
    });
}
export interface FeedApiCreateFeedResponse {
    success: boolean,
    feedId: FeedId
}
export const isFeedApiCreateFeedResponse = (x: any): x is FeedApiCreateFeedResponse => {
    return _validateObject(x, {
        success: isBoolean,
        feedId: isFeedId
    });
}

export interface FeedApiAppendMessagesRequest {
    feedId: FeedId,
    subfeedHash: SubfeedHash,
    messages: SubfeedMessage[]
}
export const isFeedApiAppendMessagesRequest = (x: any): x is FeedApiAppendMessagesRequest => {
    return _validateObject(x, {
        feedId: isFeedId,
        subfeedHash: isSubfeedHash,
        messages: isArrayOf(isSubfeedMessage)
    });
}
export interface FeedApiAppendMessagesResponse {
    success: boolean
}
export const isFeedApiAppendMessagesResponse = (x: any): x is FeedApiAppendMessagesResponse => {
    return _validateObject(x, {
        success: isBoolean
    });
}

export interface FeedApiGetNumLocalMessagesRequest {
    feedId: FeedId,
    subfeedHash: SubfeedHash
}
export const isFeedApiGetNumLocalMessagesRequest = (x: any): x is FeedApiGetNumLocalMessagesRequest => {
    return _validateObject(x, {
        feedId: isFeedId,
        subfeedHash: isSubfeedHash
    });
}
export interface FeedApiGetNumLocalMessagesResponse {
    success: boolean,
    numMessages: MessageCount
}
export const isFeedApiGetNumLocalMessagesResponse = (x: any): x is FeedApiGetNumLocalMessagesResponse => {
    return _validateObject(x, {
        success: isBoolean,
        numMessages: isMessageCount
    });
}

export interface FeedApiGetFeedInfoRequest {
    feedId: FeedId
}
export const isFeedApiGetFeedInfoRequest = (x: any): x is FeedApiGetFeedInfoRequest => {
    return _validateObject(x, {
        feedId: isFeedId
    });
}
export interface FeedApiGetFeedInfoResponse {
    success: boolean,
    isWriteable: boolean,
}
export const isFeedApiGetFeedInfoResponse = (x: any): x is FeedApiGetFeedInfoResponse => {
    return _validateObject(x, {
        success: isBoolean,
        isWriteable: isBoolean
    })
}

export interface FeedApiDeleteFeedRequest {
    feedId: FeedId
}
export const isFeedApiDeleteFeedRequest = (x: any): x is FeedApiDeleteFeedRequest => {
    return _validateObject(x, {
        feedId: isFeedId
    });
}
export interface FeedApiDeleteFeedResponse {
    success: boolean
}
export const isFeedApiDeleteFeedResponse = (x: any): x is FeedApiDeleteFeedResponse => {
    return _validateObject(x, {
        success: isBoolean
    });
}

export interface FeedApiGetFeedIdRequest {
    feedName: FeedName
}
export const isFeedApiGetFeedIdRequest = (x: any): x is FeedApiGetFeedIdRequest => {
    return _validateObject(x, {
        feedName: isFeedName
    });
}
export interface FeedApiGetFeedIdResponse {
    success: boolean,
    feedId: FeedId | null
}
export const isFeedApiGetFeedIdResponse = (x: any): x is FeedApiGetFeedIdResponse => {
    return _validateObject(x, {
        success: isBoolean,
        feedId: isOneOf([isNull, isFeedId])
    });
}

export default class DaemonApiServer {
    #node: KacheryDaemonNode
    #app: Express
    // #server: http.Server | https.Server | null = null
    #server: HttpServerInterface | null = null
    #simpleGetHandlers: {
        path: string,
        handler: (query: JSONObject) => Promise<JSONObject>,
        browserAccess: boolean
    }[] = [
        {
            // /probe - check whether the daemon is up and running and return info such as the node ID
            path: '/probe',
            handler: async (query) => {
                /* istanbul ignore next */
                return await this._handleProbe()
            },
            browserAccess: true
        },
        {
            // /halt - halt the kachery daemon (stops the server process)
            path: '/halt',
            handler: async (query) => {
                /* istanbul ignore next */
                return await this._handleHalt()
            },
            browserAccess: false
        },
        {
            path: '/stats',
            handler: async (query) => {
                return await this._handleStats(query)
            },
            browserAccess: true
        }
    ]
    #simplePostHandlers: {
        path: string,
        handler: (reqData: JSONObject) => Promise<JSONObject>,
        browserAccess: boolean
    }[] = [
        {
            // /probe - check whether the daemon is up and running and return info such as the node ID
            path: '/probe',
            handler: async (reqData: JSONObject) => {
                /* istanbul ignore next */
                return await this._handleProbe()
            },
            browserAccess: true
        },
        {
            // /storeFile - Store a local file in local kachery storage
            path: '/storeFile',
            handler: async (reqData: JSONObject) => {
                /* istanbul ignore next */
                return await this._handleStoreFile(reqData)
            },
            browserAccess: false
        },
        {
            // /linkFile - Link a local file in local kachery storage
            path: '/linkFile',
            handler: async (reqData: JSONObject) => {
                /* istanbul ignore next */
                return await this._handleLinkFile(reqData)
            },
            browserAccess: false
        },
        {
            // /feed/createFeed - create a new writeable feed on this node
            path: '/feed/createFeed',
            handler: async (reqData: JSONObject) => {return await this._handleFeedApiCreateFeed(reqData)},
            browserAccess: false
        },
        {
            // /feed/deleteFeed - delete feed on this node
            path: '/feed/deleteFeed',
            handler: async (reqData: JSONObject) => {return await this._handleFeedApiDeleteFeed(reqData)},
            browserAccess: false
        },
        {
            // /feed/getFeedId - lookup the ID of a local feed based on its name
            path: '/feed/getFeedId',
            handler: async (reqData: JSONObject) => {return await this._handleFeedApiGetFeedId(reqData)},
            browserAccess: true
        },
        {
            // /feed/appendMessages - append messages to a local writeable subfeed
            path: '/feed/appendMessages',
            handler: async (reqData: JSONObject) => {return await this._handleFeedApiAppendMessages(reqData)},
            browserAccess: true
        },
        {
            // /feed/getNumLocalMessages - get number of messages in a subfeed
            path: '/feed/getNumLocalMessages',
            handler: async (reqData: JSONObject) => {return await this._handleFeedApiGetNumLocalMessages(reqData)},
            browserAccess: true
        },
        {
            // /feed/getFeedInfo - get info for a feed - such as whether it is writeable
            path: '/feed/getFeedInfo',
            handler: async (reqData: JSONObject) => {return await this._handleFeedApiGetFeedInfo(reqData)},
            browserAccess: true
        },
        {
            // /feed/watchForNewMessages - wait until new messages have been appended to a list of watched subfeeds
            path: '/feed/watchForNewMessages',
            handler: async (reqData: JSONObject) => {return await this._handleFeedApiWatchForNewMessages(reqData)},
            browserAccess: true
        },
        {
            // /mutable/get - get a mutable value
            path: '/mutable/get',
            handler: async (reqData: JSONObject) => {return await this._handleMutableApiGet(reqData)},
            browserAccess: true
        },
        {
            // /mutable/set - set a mutable value
            path: '/mutable/set',
            handler: async (reqData: JSONObject) => {return await this._handleMutableApiSet(reqData)},
            browserAccess: true
        }
    ]

    // This is the API server for the local daemon
    // The local Python code communicates with the daemon
    // via this API
    constructor(node: KacheryDaemonNode, opts: {verbose: number}) {
        this.#node = node; // The kachery daemon
        this.#app = express(); // the express app

        this.#app.set('json spaces', 4); // when we respond with json, this is how it will be formatted

        var corsOptions = {
            origin: 'http://localhost:3000'
        }
        const cors1 = cors(corsOptions)

        this.#app.use(cors1); // in the future, if we want to do this
        this.#app.use(express.json());

        const dummyMiddleware = (req: Request, res: Response, next: () => void) => {next()}
        
        this.#simpleGetHandlers.forEach(h => {
            this.#app.get(h.path, async (req, res) => {
                if (h.path !== '/probe') {
                    if (!this._checkAuthCode(req, res, {browserAccess: h.browserAccess})) return
                }
                /////////////////////////////////////////////////////////////////////////
                /* istanbul ignore next */
                await action(h.path, {context: 'Daemon API'}, async () => {
                    const response = await h.handler(req.query as any as JSONObject)
                    if (response.format === 'html') {
                        res.end(response.html)
                    }
                    else {
                        res.json(response)
                    }
                }, async (err: Error) => {
                    await this._errorResponse(req, res, 500, err.message);
                });
                /////////////////////////////////////////////////////////////////////////
            })
        })

        this.#simplePostHandlers.forEach(h => {
            this.#app.post(h.path, async (req, res) => {
                if (!this._checkAuthCode(req, res, {browserAccess: h.browserAccess})) return
                /////////////////////////////////////////////////////////////////////////
                /* istanbul ignore next */
                await action(h.path, {context: 'Daemon API'}, async () => {
                    const reqData = req.body
                    if (!isJSONObject(reqData)) throw Error ('Not a JSONObject')
                    const response = await h.handler(reqData)
                    res.json(response)
                }, async (err: Error) => {
                    await this._errorResponse(req, res, 500, err.message);
                });
                /////////////////////////////////////////////////////////////////////////
            })
        })

        // // /findFile - find a file (or feed) in the remote nodes. May return more than one.
        // this.#app.post('/findFile', async (req, res) => {
        //     if (!this._checkAuthCode(req, res, {browserAccess: true})) return
        //     /////////////////////////////////////////////////////////////////////////
        //     /* istanbul ignore next */
        //     await action('/findFile', {context: 'Daemon API'}, async () => {
        //         await this._apiFindFile(req, res)
        //     }, async (err: Error) => {
        //         await this._errorResponse(req, res, 500, err.message);
        //     });
        //     /////////////////////////////////////////////////////////////////////////
        // });
        // /loadFile - download file from remote node(s) and store in kachery storage
        this.#app.post('/loadFile', async (req, res) => {
            if (!this._checkAuthCode(req, res, {browserAccess: true})) return
            /////////////////////////////////////////////////////////////////////////
            /* istanbul ignore next */
            await action('/loadFile', {context: 'Daemon API'}, async () => {
                await this._apiLoadFile(req, res)
            }, async (err: Error) => {
                res.status(500).send('Error loading file.');
            });
            /////////////////////////////////////////////////////////////////////////
        });
        // /downloadFileData - download file data - file must exist in local kachery storage
        this.#app.post('/downloadFileData', async (req, res) => {
            if (!this._checkAuthCode(req, res, {browserAccess: true})) return
            /////////////////////////////////////////////////////////////////////////
            /* istanbul ignore next */
            await action('/downloadFileData', {context: 'Daemon API'}, async () => {
                await this._apiDownloadFileData(req, res)
            }, async (err: Error) => {
                res.status(500).send(`Error downloading file: ${err.message}`);
            });
            /////////////////////////////////////////////////////////////////////////
        });
        // /store - store a file by streaming data to the daemon
        this.#app.post('/store', async (req, res) => {
            if (!this._checkAuthCode(req, res, {browserAccess: true})) return
            /////////////////////////////////////////////////////////////////////////
            /* istanbul ignore next */
            await action('/store', {context: 'Daemon API'}, async () => {
                await this._apiStore(req, res)
            }, async (err: Error) => {
                res.status(500).send('Error storing file from data stream.');
            });
            /////////////////////////////////////////////////////////////////////////
        });
    }
    stop() {
        /* istanbul ignore next */
        if (this.#server) {
            this.#server.close()
        }
    }
    // async mockGetJson(path: string): Promise<JSONObject> {
    //     for (let h of this.#simpleGetHandlers) {
    //         if (h.path === path) {
    //             return await h.handler()
    //         }
    //     }
    //     throw Error(`Unexpected path in mockGetJson: ${path}`)
    // }
    async mockPostJson(path: string, data: JSONObject): Promise<JSONObject> {
        for (let h of this.#simplePostHandlers) {
            if (h.path === path) {
                return await h.handler(data)
            }
        }
        /* istanbul ignore next */
        throw Error(`Unexpected path in mockPostJson: ${path}`)
    }
    async mockPostLoadFile(data: JSONObject): Promise<DataStreamy> {
        /* istanbul ignore next */
        if (!isApiLoadFileRequest(data)) throw Error('Unexpected data in mockPostLoadFile')
        return await this._loadFile(data)
    }
    // /probe - check whether the daemon is up and running and return info such as the node ID
    /* istanbul ignore next */
    async _handleProbe(): Promise<JSONObject> {
        const response: DaemonApiProbeResponse = {
            success: true,
            daemonVersion: daemonVersion,
            nodeId: this.#node.nodeId(),
            kacheryStorageDir: this.#node.kacheryStorageManager().storageDir()
        }
        /* istanbul ignore next */
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object');
        return response
    }
    // /storeFile - store local file in local kachery storage
    /* istanbul ignore next */
    async _handleStoreFile(reqData: JSONObject): Promise<JSONObject> {
        if (!isStoreFileRequestData(reqData)) throw Error('Unexpected request data for storeFile.')
        
        const {sha1, manifestSha1} = await this.#node.kacheryStorageManager().storeLocalFile(reqData.localFilePath)
        const response: StoreFileResponseData = {
            success: true,
            error: null,
            sha1,
            manifestSha1
        }
        /* istanbul ignore next */
        if (!isJSONObject(response)) throw Error('Unexpected json object in _handleStoreFile')
        return response
    }
    // /linkFile - link local file in local kachery storage
    /* istanbul ignore next */
    async _handleLinkFile(reqData: JSONObject): Promise<JSONObject> {
        if (!isLinkFileRequestData(reqData)) throw Error('Unexpected request data for linkFile.')
        
        const {sha1, manifestSha1} = await this.#node.kacheryStorageManager().linkLocalFile(reqData.localFilePath, {size: reqData.size, mtime: reqData.mtime})
        const response: LinkFileResponseData = {
            success: true,
            error: null,
            sha1,
            manifestSha1
        }
        /* istanbul ignore next */
        if (!isJSONObject(response)) throw Error('Unexpected json object in _handleLinkFile')
        return response
    }
    // /halt - halt the kachery daemon (stops the server process)
    /* istanbul ignore next */
    async _handleHalt(): Promise<JSONObject> {
        interface ApiHaltResponse {
            success: boolean
        };
        this.stop()
        const response: ApiHaltResponse = { success: true };
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object');
        setTimeout(() => {
            process.exit()
        }, durationMsecToNumber(scaledDurationMsec(3000)))
        return response
    }
    // /stats
    async _handleStats(query: JSONObject): Promise<JSONObject> {
        /* istanbul ignore next */
        if (!isGetStatsOpts(query)) throw Error('Unexpected query.')
        interface ApiStatsResponse {
            success: boolean,
            format: string,
            html?: string,
            stats: NodeStatsInterface
        }
        const stats = this.#node.getStats(query)
        const response: ApiStatsResponse = {
            success: true,
            format: (query.format || 'json') as string,
            html: stats.html,
            stats
        }
        /* istanbul ignore next */
        if (!isJSONObject(response)) throw Error('Unexpected json object in _handleStats')
        return response
    }
    // /loadFile - load a file from remote kachery node(s) and store in kachery storage
    /* istanbul ignore next */
    async _apiLoadFile(req: Request, res: Response) {
        const jsonSocket = new JsonSocket(res as any as Socket)
        let x: DataStreamy
        const apiLoadFileRequest = req.body
        if (!isApiLoadFileRequest(apiLoadFileRequest)) {
            jsonSocket.sendMessage({type: 'error', error: 'Invalid api load file request'}, () => {})
            res.end()
            return
        }
        try {
            x = await this._loadFile(apiLoadFileRequest)
        }
        catch(err) {
            jsonSocket.sendMessage({type: 'error', error: err.message}, () => {})
            res.end()
            return
        }
        let lastProgressTimestamp = nowTimestamp()
        let isDone = false
        x.onFinished(() => {
            if (isDone) return
            // we are done
            jsonSocket.sendMessage({
                type: 'progress',
                bytesLoaded: x.bytesLoaded(),
                bytesTotal: x.bytesTotal() || x.bytesLoaded()
            }, () => {})
            const localFilePath = apiLoadFileRequest.fileKey.sha1
            this.#node.kacheryStorageManager().findFile(apiLoadFileRequest.fileKey).then(({found, size, localFilePath}) => {
                if (isDone) return
                if (found) {
                    if (localFilePath) {
                        isDone = true
                        jsonSocket.sendMessage({type: 'finished', localFilePath}, () => {})
                        res.end()
                    }
                    else {
                        isDone = true
                        jsonSocket.sendMessage({type: 'error', error: 'Unexpected: load completed, but localFilePath is null.'}, () => {})
                        res.end()
                    }
                }
                else {
                    isDone = true
                    jsonSocket.sendMessage({type: 'error', error: 'Unexpected: did not find file in local kachery storage even after load completed'}, () => {})
                    res.end()
                }
            })
        });
        x.onError((err) => {
            if (isDone) return
            isDone = true
            jsonSocket.sendMessage({type: 'error', error: err.message}, () => {})
            res.end()
        });
        x.onProgress((prog) => {
            const elapsed = elapsedSince(lastProgressTimestamp)
            if (elapsed > 2000) {
                lastProgressTimestamp = nowTimestamp()
                jsonSocket.sendMessage({
                    type: 'progress',
                    bytesLoaded: prog.bytesLoaded,
                    bytesTotal: prog.bytesTotal
                }, () => {})
            }
        });
        req.on('close', () => {
            // if the request socket is closed, we cancel the load request
            isDone = true
            x.cancel()
        });
    }
    // /loadFile - download data for a file - must already be on this node
    /* istanbul ignore next */
    async _apiDownloadFileData(req: Request, res: Response): Promise<void> {
        const apiDownloadFileDataRequest = req.body
        if (!isApiDownloadFileDataRequest(apiDownloadFileDataRequest)) {
            throw Error('Invalid request in _apiDownloadFileData');
        }
        const x = await this.#node.kacheryStorageManager().getFileDataStreamy(apiDownloadFileDataRequest.fileKey, apiDownloadFileDataRequest.startByte, apiDownloadFileDataRequest.endByte)
        return new Promise((resolve, reject) => {
            x.onData((chunk: Buffer) => {
                res.write(chunk)
            })
            x.onError((err: Error) => {
                reject(err.message)
            })
            x.onFinished(() => {
                res.end()
            })
        })
    }
    // /store - store file by streaming data to daemon
    /* istanbul ignore next */
    async _apiStore(req: Request, res: Response) {
        const contentLength = req.header("Content-Length")
        if (!contentLength) {
            res.status(403).send("Missing Content-Length in header").end();
            return
        }
        const fileSize = parseInt(contentLength)
        const x = new DataStreamy()
        req.on('data', (chunk: Buffer) => {
            x.producer().data(chunk)
        })
        req.on('end', () => {
            x.producer().end()
        })
        req.on('error', (err: Error) => {
            x.producer().error(err)
        })
        let response: StoreFileResponseData
        try {
            const {sha1, manifestSha1} = await this.#node.kacheryStorageManager().storeFileFromStream(x, byteCount(fileSize), {calculateHashOnly: false})
            response = {
                success: true,
                error: null,
                sha1,
                manifestSha1
            }
        }
        catch(err) {
            response = {
                success: true,
                error: err.message,
                sha1: null,
                manifestSha1: null
            }
        }
        /* istanbul ignore next */
        if (!isJSONObject(response)) throw Error('Unexpected json object in _handleStoreFile')
        res.json(response)
    }
    async _loadFile(reqData: ApiLoadFileRequest) {
        /* istanbul ignore next */
        if (!isApiLoadFileRequest(reqData)) throw Error('Invalid request in _apiLoadFile');

        const { fileKey } = reqData;
        if (fileKey.manifestSha1) {
            console.info(`Loading file: sha1://${fileKey.sha1}?manifest=${fileKey.manifestSha1}`)
        }
        else {
            console.info(`Loading file: sha1://${fileKey.sha1}`)
        }        
        const x = await loadFile(
            this.#node,
            fileKey,
            {label: fileKey.sha1.toString().slice(0, 5)}
        )
        return x
    }
    // /feed/createFeed - create a new writeable feed on this node
    async _handleFeedApiCreateFeed(reqData: any) {
        /* istanbul ignore next */
        if (!isFeedApiCreateFeedRequest(reqData)) throw Error('Invalid request in _feedApiCreateFeed');

        const feedName = reqData.feedName || null;
        const feedId = await this.#node.feedManager().createFeed({feedName});
        const response: FeedApiCreateFeedResponse = { success: true, feedId };
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object');
        return response
    }
    // /feed/deleteFeed - delete feed on this node
    async _handleFeedApiDeleteFeed(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isFeedApiDeleteFeedRequest(reqData)) throw Error('Invalid request in _feedApiDeleteFeed');

        const { feedId } = reqData;
        await this.#node.feedManager().deleteFeed({feedId});

        const response: FeedApiDeleteFeedResponse = {success: true}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object');
        return response
    }
    // /feed/getFeedId - lookup the ID of a local feed based on its name
    async _handleFeedApiGetFeedId(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isFeedApiGetFeedIdRequest(reqData)) throw Error('Invalid request in _feedApiGetFeedId');
        const { feedName } = reqData;
        const feedId = await this.#node.feedManager().getFeedId({feedName});
        let response: FeedApiGetFeedIdResponse;
        if (!feedId) {
            response = { success: false, feedId: null };
        }
        else {
            response = { success: true, feedId };
        }
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object');
        return response
    }
    // /feed/appendMessages - append messages to a local writeable subfeed
    async _handleFeedApiAppendMessages(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isFeedApiAppendMessagesRequest(reqData)) throw Error('Invalid request in _feedApiAppendMessages')
        const { feedId, subfeedHash, messages } = reqData

        // CHAIN:append_messages:step(2)
        await this.#node.feedManager().appendMessages({
            feedId, subfeedHash, messages
        });

        const response: FeedApiAppendMessagesResponse = {success: true}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // /feed/getNumLocalMessages - get number of messages in a subfeed
    async _handleFeedApiGetNumLocalMessages(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isFeedApiGetNumLocalMessagesRequest(reqData)) throw Error('Invalid request in _feedApiGetNumLocalMessages');

        const { feedId, subfeedHash } = reqData;

        const numMessages = await this.#node.feedManager().getNumLocalMessages({
            feedId, subfeedHash
        });

        const response: FeedApiGetNumLocalMessagesResponse = {success: true, numMessages}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object');
        return response
    }
    // /feed/getFeedInfo - get info for a feed - such as whether it is writeable
    async _handleFeedApiGetFeedInfo(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isFeedApiGetFeedInfoRequest(reqData)) throw Error('Invalid request in _feedApiGetFeedInfo');

        const { feedId } = reqData;
        const isWriteable = await this.#node.feedManager().hasWriteableFeed(feedId)

        const response: FeedApiGetFeedInfoResponse = {success: true, isWriteable: isWriteable}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object');
        return response
    }
    // /feed/watchForNewMessages - wait until new messages have been appended to a list of watched subfeeds
    async _handleFeedApiWatchForNewMessages(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isFeedApiWatchForNewMessagesRequest(reqData)) throw Error('Invalid request in _feedApiWatchForNewMessages')

        const { subfeedWatches, waitMsec, maxNumMessages, signed } = reqData

        console.log('--- w1')
        const messages = await this.#node.feedManager().watchForNewMessages({
            subfeedWatches: toSubfeedWatchesRAM(subfeedWatches), waitMsec, maxNumMessages: maxNumMessages || messageCount(0), signed: signed || false
        })

        const response: FeedApiWatchForNewMessagesResponse = {success: true, messages: mapToObject(messages)}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // /mutable/set - set a mutable value
    async _handleMutableApiSet(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isMutableApiSetRequest(reqData)) throw Error('Invalid request in _mutableApiSet')
        const { key, value } = reqData

        await this.#node.mutableManager().set(key, value)

        const response: MutableApiSetResponse = {success: true}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // /mutable/get - get a mutable value
    async _handleMutableApiGet(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isMutableApiGetRequest(reqData)) throw Error('Invalid request in _mutableApiGet')
        const { key } = reqData

        const rec = await this.#node.mutableManager().get(key)

        const response: MutableApiGetResponse = {success: true, found: rec !== undefined,  value: rec !== undefined ? rec.value : ''}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // /mutable/delete - delete a mutable value
    async _handleMutableApiDelete(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isMutableApiDeleteRequest(reqData)) throw Error('Invalid request in _mutableApiDelete')
        const { key } = reqData

        await this.#node.mutableManager().delete(key)

        const response: MutableApiDeleteResponse = {success: true}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // Helper function for returning http request with an error response
    /* istanbul ignore next */
    async _errorResponse(req: Request, res: Response, code: number, errorString: string) {
        console.info(`Daemon responding with error: ${code} ${errorString}`);
        try {
            res.status(code).send(errorString);
        }
        catch(err) {
            console.warn(`Problem sending error`, {error: err.message});
        }
        await sleepMsec(scaledDurationMsec(100));
        try {
            req.socket.destroy();
        }
        catch(err) {
            console.warn('Problem destroying connection', {error: err.message});
        }
    }
    // Start listening via http/https
    async listen(port: Port) {
        this.#server = await this.#node.externalInterface().startHttpServer(this.#app, port)
    }
    _checkAuthCode(req: Request, res: Response, opts: {browserAccess: boolean}) {
        const authCode = req.header('KACHERY-CLIENT-AUTH-CODE')
        if (!authCode) {
            res.status(403).send("Missing client auth code in daemon request. You probably need to upgrade kachery-daemon or kachery.").end();
            return false
        }
        if (!this.#node.verifyClientAuthCode(authCode, {browserAccess: opts.browserAccess})) {
            res.status(403).send("Incorrect or invalid client authorization code.").end();
            return false
        }
        return true
    }
}

const isLocalRequest = (req: Request) => {
    return (req.socket.localAddress === req.socket.remoteAddress);
}