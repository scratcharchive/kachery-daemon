import axios from "axios";
import GarbageMap from "../common/GarbageMap";
import { ByteCount, FileKey, fileKeyHash, scaledDurationMsec, Sha1Hash, sha1OfObject, UrlString } from "../common/types/kacheryTypes";
import { KacheryStorageManagerInterface } from "../external/ExternalInterface";

export type SignedFileUploadUrlCallback = (a: {channelName: string, sha1: Sha1Hash, size: ByteCount}) => Promise<UrlString>

class FileUploadTask {
    #complete = false
    #onCompleteCallbacks: (() => void)[] = []
    #status: 'waiting' | 'running' | 'finished' | 'error' = 'waiting'
    #error: Error | undefined = undefined
    constructor(private channelName: string, private fileKey: FileKey, private fileSize: ByteCount, private signedFileUploadUrlCallback: SignedFileUploadUrlCallback, private kacheryStorageManager: KacheryStorageManagerInterface) {
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
            const url = await this.signedFileUploadUrlCallback({channelName: this.channelName, sha1: this.fileKey.sha1, size: this.fileSize})
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
    constructor(private signedFileUploadUrlCallback: SignedFileUploadUrlCallback, private kacheryStorageManager: KacheryStorageManagerInterface) {

    }
    getTask(channelName: string, fileKey: FileKey, fileSize: ByteCount) {
        const code = sha1OfObject({channelName, fileKeyHash: fileKeyHash(fileKey).toString()})
        const t = this.#tasks.get(code)
        if (t) return t
        const newTask = new FileUploadTask(channelName, fileKey, fileSize, this.signedFileUploadUrlCallback, this.kacheryStorageManager)
        this.#tasks.set(code, newTask)
        return newTask
    }
}

class FileUploader {
    #taskManager: FileUploadTaskManager
    constructor(private signedFileUploadUrlCallback: SignedFileUploadUrlCallback, private kacheryStorageManager: KacheryStorageManagerInterface) {
        this.#taskManager = new FileUploadTaskManager(this.signedFileUploadUrlCallback, this.kacheryStorageManager)
    }
    async uploadFileToBucket(args: {channelName: string, fileKey: FileKey, fileSize: ByteCount}) {
        const {channelName, fileKey, fileSize} = args
        const task = this.#taskManager.getTask(channelName, fileKey, fileSize)
        await task.wait()
    }
}

export default FileUploader