import axios from "axios";
import Stream from 'stream';
import GarbageMap from "../common/GarbageMap";
import { ByteCount, FileKey, fileKeyHash, scaledDurationMsec, Sha1Hash, sha1OfObject, UrlString } from "../common/types/kacheryTypes";
import { KacheryStorageManagerInterface } from "../external/ExternalInterface";

export type SignedUploadUrlCallback = (bucketUri: string, sha1: Sha1Hash, size: ByteCount) => Promise<UrlString>

class FileUploadTask {
    #complete = false
    #onCompleteCallbacks: (() => void)[] = []
    #status: 'waiting' | 'running' | 'finished' | 'error' = 'waiting'
    #error: Error | undefined = undefined
    constructor(private bucketUri: string, private fileKey: FileKey, private fileSize: ByteCount, private signedUploadUrlCallback: SignedUploadUrlCallback, private kacheryStorageManager: KacheryStorageManagerInterface) {
        this._start()
    }
    async wait() {
        if (!this.#complete) {
            await new Promise<void>((resolve) => {
                if (this.#complete) return
                this.#onCompleteCallbacks.push(() => {
                    resolve()
                })
            })
        }
        if (this.#status === 'finished') {
            return
        }
        else if (this.#status === 'error') {
            throw this.#error
        }
        else {
            throw Error(`Unexpected status for completed: ${this.#status}`)
        }
    }
    async _start() {
        this.#status = 'running'
        try {
            const url = await this.signedUploadUrlCallback(this.bucketUri, this.fileKey.sha1, this.fileSize)
            const {stream: dataStream, size} = await this.kacheryStorageManager.getFileReadStream(this.fileKey)
            const resp = await axios.put(url.toString(), dataStream, {
                headers: {
                    'Content-Type': 'application/octet-stream',
                    'Content-Length': Number(size)
                },
                maxBodyLength: Infinity, // apparently this is important
                maxContentLength: Infinity // apparently this is important
            })
            if (resp.status !== 200) {
                throw Error(`Error in upload: ${resp.statusText}`)
            }
            this.#status = 'finished'
        }
        catch(err) {
            this.#error = err
            this.#status = 'error'
        }
        this.#complete = true
        this.#onCompleteCallbacks.forEach(cb => cb())
    }
}

class FileUploadTaskManager {
    #tasks = new GarbageMap<Sha1Hash, FileUploadTask>(scaledDurationMsec(1000 * 60 * 60))
    constructor(private signedUploadUrlCallback: SignedUploadUrlCallback, private kacheryStorageManager: KacheryStorageManagerInterface) {

    }
    getTask(bucketUri: string, fileKey: FileKey, fileSize: ByteCount) {
        const code = sha1OfObject({bucketUri, fileKeyHash: fileKeyHash(fileKey).toString()})
        const t = this.#tasks.get(code)
        if (t) return t
        const newTask = new FileUploadTask(bucketUri, fileKey, fileSize, this.signedUploadUrlCallback, this.kacheryStorageManager)
        this.#tasks.set(code, newTask)
        return newTask
    }
}

class FileUploader {
    #taskManager: FileUploadTaskManager
    constructor(private signedUploadUrlCallback: SignedUploadUrlCallback, private kacheryStorageManager: KacheryStorageManagerInterface) {
        this.#taskManager = new FileUploadTaskManager(this.signedUploadUrlCallback, this.kacheryStorageManager)
    }
    async uploadFileToBucket(args: {bucketUri: string, fileKey: FileKey, fileSize: ByteCount}) {
        const {bucketUri, fileKey, fileSize} = args
        const task = this.#taskManager.getTask(bucketUri, fileKey, fileSize)
        await task.wait()
    }
}

export default FileUploader