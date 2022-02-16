import cors from 'cors';
import express, { Express, Request, Response } from 'express';
import JsonSocket from 'json-socket';
import { Socket } from 'net';
import { action } from './action';
import DataStreamy from '../commonInterface/util/DataStreamy';
import { byteCount, durationMsecToNumber, elapsedSince, FileKey, isJSONObject, JSONObject, mapToObject, messageCount, nowTimestamp, Port, scaledDurationMsec, toSubfeedWatchesRAM, UrlString } from '../commonInterface/kacheryTypes';
import { sleepMsec } from '../commonInterface/util/util';
import daemonVersion from '../daemonVersion';
import ExternalInterface, { HttpServerInterface } from '../kacheryInterface/core/ExternalInterface';
import { isGetStatsOpts, NodeStatsInterface } from '../kacheryInterface/core/getStats';
import KacheryNode from '../kacheryInterface/core/KacheryNode';
import { loadFile } from '../kacheryInterface/core/loadFile';
import { ApiLoadFileRequest, DaemonApiProbeResponse, FeedApiAppendMessagesResponse, FeedApiCreateFeedResponse, FeedApiDeleteFeedResponse, FeedApiGetFeedIdResponse, FeedApiGetFeedInfoResponse, FeedApiGetNumLocalMessagesResponse, FeedApiWatchForNewMessagesResponse, isApiDownloadFileDataRequest, isApiLoadFileRequest, isFeedApiAppendMessagesRequest, isFeedApiCreateFeedRequest, isFeedApiDeleteFeedRequest, isFeedApiGetFeedIdRequest, isFeedApiGetFeedInfoRequest, isFeedApiGetNumLocalMessagesRequest, isFeedApiWatchForNewMessagesRequest, isLinkFileRequestData, isMutableApiDeleteRequest, isMutableApiGetRequest, isMutableApiSetRequest, isStoreFileRequestData, isTaskCreateSignedTaskResultUploadUrlRequest, isTaskRegisterTaskFunctionsRequest, isTaskRequestTaskRequest, isTaskUpdateTaskStatusRequest, isTaskWaitForTaskResultRequest, isUploadFileRequestData, LinkFileResponseData, MutableApiDeleteResponse, MutableApiGetResponse, MutableApiSetResponse, StoreFileResponseData, TaskCreateSignedTaskResultUploadUrlResponse, TaskRegisterTaskFunctionsResponse, TaskRequestTaskResponse, TaskUpdateTaskStatusResponse, TaskWaitForTaskResultResponse, UploadFileResponseData, isCreateSignedFileUploadUrlRequest, CreateSignedFileUploadUrlResponse } from './daemonApiTypes';
import { RequestedTask } from '../kacheryInterface/kacheryHubTypes';
import logger from "winston";

export default class DaemonApiServer {
    #node: KacheryNode
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
            // /uploadFile - Upload a stored file to a kachery channel
            path: '/uploadFile',
            handler: async (reqData: JSONObject) => {
                /* istanbul ignore next */
                return await this._handleUploadFile(reqData)
            },
            browserAccess: false
        },
        {
            // /createSignedFileUploadUrl
            path: '/createSignedFileUploadUrl',
            handler: async (reqData: JSONObject) => {return await this._handleCreateSignedFileUploadUrl(reqData)},
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
        },
        {
            // /mutable/delete - delete a mutable value
            path: '/mutable/delete',
            handler: async (reqData: JSONObject) => {return await this._handleMutableApiDelete(reqData)},
            browserAccess: true
        },
        {
            // /task/registerTaskFunctions
            path: '/task/registerTaskFunctions',
            handler: async (reqData: JSONObject) => {return await this._handleTaskRegisterTaskFunctions(reqData)},
            browserAccess: true
        },
        {
            // /task/updateTaskStatus
            path: '/task/updateTaskStatus',
            handler: async (reqData: JSONObject) => {return await this._handleTaskUpdateTaskStatus(reqData)},
            browserAccess: true
        },
        {
            // /task/createSignedTaskResultUploadUrl
            path: '/task/createSignedTaskResultUploadUrl',
            handler: async (reqData: JSONObject) => {return await this._handleTaskCreateSignedTaskResultUploadUrl(reqData)},
            browserAccess: true
        },
        {
            // /task/requestTask
            path: '/task/requestTask',
            handler: async (reqData: JSONObject) => {return await this._handleTaskRequestTask(reqData)},
            browserAccess: true
        },
        {
            // /task/waitForTaskResult
            path: '/task/waitForTaskResult',
            handler: async (reqData: JSONObject) => {return await this._handleTaskWaitForTaskResult(reqData)},
            browserAccess: true
        }
    ]

    // This is the API server for the local daemon
    // The local Python code communicates with the daemon
    // via this API
    constructor(node: KacheryNode, private externalInterface: ExternalInterface, opts: {verbose: number}) {
        this.#node = node; // The kachery daemon
        this.#app = express(); // the express app

        this.#app.set('json spaces', 4); // when we respond with json, this is how it will be formatted

        var corsOptions = {
            origin: 'http://localhost:3000'
        }
        const cors1 = cors(corsOptions)

        this.#app.use(cors1); // in the future, if we want to do this
        this.#app.use(express.json());

        // const dummyMiddleware = (req: Request, res: Response, next: () => void) => {next()}
        
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
    // /uploadFile - Upload a stored file to a kachery channel
    /* istanbul ignore next */
    async _handleUploadFile(reqData: JSONObject): Promise<JSONObject> {
        if (!isUploadFileRequestData(reqData)) throw Error('Unexpected request data for uploadFile.')
        
        const fileKey: FileKey = {
            sha1: reqData.sha1
        }
        const {found, size, localFilePath} = await this.#node.kacheryStorageManager().findFile(fileKey)
        if (!found) throw Error('uploadFile: File not found in kachery storage')
        if (!size) throw Error('uploadFile: Size is zero or null.')
        if (Number(size) > 20 * 1000 * 1000) throw Error('uploadFile: File too large for upload')
        await this.#node.uploadFile({fileKey, channelName: reqData.channel, fileSize: size})
        const response: UploadFileResponseData = {
            success: true
        }
        /* istanbul ignore next */
        if (!isJSONObject(response)) throw Error('Unexpected json object in _handleUploadFile')
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
    // /loadFileData - download data for a file - must already be on this node
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
            logger.info(`Loading file: sha1://${fileKey.sha1}?manifest=${fileKey.manifestSha1}`)
        }
        else {
            logger.info(`Loading file: sha1://${fileKey.sha1}`)
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
        const update = reqData.update !== undefined ? reqData.update : true // default true

        const success = await this.#node.mutableManager().set(key, value, {update})

        const response: MutableApiSetResponse = {success}
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

        const success = await this.#node.mutableManager().delete(key)

        const response: MutableApiDeleteResponse = {success}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // /task/registerTaskFunctions
    async _handleTaskRegisterTaskFunctions(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isTaskRegisterTaskFunctionsRequest(reqData)) throw Error('Invalid request in _handleTaskRegisterTaskFunctions')
        const { taskFunctions, backendId, timeoutMsec } = reqData

        const requestedTasks: RequestedTask[] = await this.#node.kacheryHubInterface().registerTaskFunctions({taskFunctions, timeoutMsec, backendId: backendId || null})

        const response: TaskRegisterTaskFunctionsResponse = {success: true, requestedTasks}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')

        return response
    }
    // /task/updateTaskStatus
    async _handleTaskUpdateTaskStatus(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isTaskUpdateTaskStatusRequest(reqData)) throw Error('Invalid request in _handleTaskUpdateTaskStatus')
        const { channelName, taskId, status, errorMessage } = reqData

        await this.#node.kacheryHubInterface().updateTaskStatus({channelName, taskId, status, errorMessage})

        const response: TaskUpdateTaskStatusResponse = {success: true}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // /task/createSignedTaskResultUploadUrl
    async _handleTaskCreateSignedTaskResultUploadUrl(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isTaskCreateSignedTaskResultUploadUrlRequest(reqData)) throw Error('Invalid request in _handleTaskCreateSignedTaskResultUploadUrl')
        const { channelName, taskId, size } = reqData

        const signedUrl = await this.#node.kacheryHubInterface().createSignedTaskResultUploadUrl({channelName, taskId, size})

        const response: TaskCreateSignedTaskResultUploadUrlResponse = {success: true, signedUrl}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // /createSignedFileUploadUrl
    async _handleCreateSignedFileUploadUrl(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isCreateSignedFileUploadUrlRequest(reqData)) throw Error('Invalid request in _handleCreateSignedFileUploadUrl')
        const { channelName, sha1 } = reqData

        let success: boolean
        let alreadyUploaded: boolean
        let signedUrl: UrlString | undefined
        const exists = await this.#node.kacheryHubInterface().checkForFileInChannelBucket(sha1, channelName)
        if (exists) {
            success = true
            alreadyUploaded = true
            signedUrl = undefined
        }
        else {
            alreadyUploaded = false
            const {found, size} = await this.#node.kacheryStorageManager().findFile({sha1})
            if (!found) {
                // sometimes we might have content to upload, but it's not in kachery storage (like chunks of a file)
                // in that case we need to have the size in the request
                if (reqData.size) {
                    success = true
                    signedUrl = await this.#node.kacheryHubInterface().createSignedFileUploadUrl({channelName, sha1, size: reqData.size})
                }
                else {
                    success = false
                }
            }
            else {
                if ((!reqData.size) || (reqData.size === size)) {
                    success = true
                    signedUrl = await this.#node.kacheryHubInterface().createSignedFileUploadUrl({channelName, sha1, size})
                }
                else {
                    // size mismatch
                    success = false
                }
            }
        }
        
        const response: CreateSignedFileUploadUrlResponse = {success, alreadyUploaded, signedUrl}
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // /task/requestTask
    async _handleTaskRequestTask(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isTaskRequestTaskRequest(reqData)) throw Error('Invalid request in _handleTaskRequestTask')
        const { channelName, taskFunctionId, taskKwargs, timeoutMsec, taskFunctionType, backendId } = reqData

        const taskId = this.#node.kacheryHubInterface().createTaskIdForTask({taskFunctionId, taskKwargs, taskFunctionType})
        const result = await this.#node.kacheryHubInterface().requestTaskFromChannel({channelName, taskId, taskFunctionId, taskKwargs, timeoutMsec, taskFunctionType, backendId: backendId || null})

        const response: TaskRequestTaskResponse = {
            success: true,
            taskId: result.taskId,
            status: result.status,
            taskResultUrl: result.taskResultUrl,
            errorMessage: result.errorMessage
        }
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // /task/waitForTaskResult
    async _handleTaskWaitForTaskResult(reqData: JSONObject) {
        /* istanbul ignore next */
        if (!isTaskWaitForTaskResultRequest(reqData)) throw Error('Invalid request in _handleTaskWaitForTaskResult')
        const { channelName, taskId, taskResultUrl, taskFunctionType, timeoutMsec } = reqData

        const result = await this.#node.kacheryHubInterface().waitForTaskResult({channelName, taskId, taskFunctionType, taskResultUrl, timeoutMsec})

        const response: TaskWaitForTaskResultResponse = {
            success: true,
            status: result.status,
            errorMessage: result.errorMessage
        }
        if (!isJSONObject(response)) throw Error('Unexpected, not a JSON-serializable object')
        return response
    }
    // Helper function for returning http request with an error response
    /* istanbul ignore next */
    async _errorResponse(req: Request, res: Response, code: number, errorString: string) {
        logger.error(`Daemon responding with error: ${code} ${errorString}`);
        try {
            res.status(code).send(errorString);
        }
        catch(err) {
            logger.warn(`Problem sending error`, {error: err.message});
        }
        await sleepMsec(scaledDurationMsec(100));
        try {
            req.socket.destroy();
        }
        catch(err) {
            logger.warn('Problem destroying connection', {error: err.message});
        }
    }
    // Start listening via http/https
    async listen(port: Port) {
        this.#server = await this.externalInterface.startHttpServer(this.#app, port)
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