import { Mutex } from "async-mutex"
import { JSONStringifyDeterministic, JSONValue, LocalFilePath, localFilePath, scaledDurationMsec, Sha1Hash, sha1OfString } from "../../../commonInterface/kacheryTypes"
import GarbageMap from "../../../commonInterface/util/GarbageMap"
import MutableDatabase, { MutableRecord } from "./MutableDatabase"

export default class MutableManager {
    // Manages the mutables
    #memoryCache = new GarbageMap<Sha1Hash, MutableRecord>(scaledDurationMsec(1000 * 60 * 10))
    #mutableDatabase: MutableDatabase
    #onSetCallbacks: ((key: JSONValue) => void)[] = []
    #mutex = new Mutex()
    constructor(storageDir: LocalFilePath) {
        this.#mutableDatabase = new MutableDatabase(localFilePath(storageDir + '/mutables.db'))
    }
    async set(key: JSONValue, value: JSONValue, opts: {update: boolean}): Promise<boolean> {
        let {update} = opts
        const sha1 = sha1OfString(JSONStringifyDeterministic(key as Object))
        if (!update) {
            // if we are not updating, let's first check whether the key has already been set
            if (this.#memoryCache.has(sha1)) return false
        }
        const rec: MutableRecord = {key, value}
        const release = await this.#mutex.acquire()
        try {
            if (!update) {
                const a = await this.#mutableDatabase.get(sha1)
                if (a) {
                    this.#memoryCache.set(sha1, a)
                    return false
                }
            }
            await this.#mutableDatabase.set(sha1, rec)
            this.#memoryCache.set(sha1, rec)
        }
        finally {
            release()
        }
        this.#onSetCallbacks.forEach(cb => {cb(key)})
        return true
    }
    async get(key: JSONValue): Promise<MutableRecord | undefined> {
        const sha1 = sha1OfString(JSONStringifyDeterministic(key as Object))
        const rec = this.#memoryCache.get(sha1)
        if (rec !== undefined) {
            return rec
        }
        const release = await this.#mutex.acquire()
        try {
            const rec2 = await this.#mutableDatabase.get(sha1)
            if (rec2) {
                this.#memoryCache.set(sha1, rec2)
            }
            return rec2
        }
        finally {
            release()
        }
    }
    async delete(key: JSONValue): Promise<boolean> {
        const a = await this.get(key)
        if (a === undefined) return false
        const sha1 = sha1OfString(JSONStringifyDeterministic(key as Object))
        const release = await this.#mutex.acquire()
        try {
            await this.#mutableDatabase.delete(sha1)
            this.#memoryCache.delete(sha1)
        }
        finally {
            release()
        }
        return true
    }
    onSet(callback: (key: JSONValue) => void) {
        this.#onSetCallbacks.push(callback)
    }
}