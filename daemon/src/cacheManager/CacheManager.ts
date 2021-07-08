import { KacheryStorageManagerInterface } from "kachery-js/core/ExternalInterface";
import { byteCount, ByteCount, isByteCount, isNumber, isObject, isSha1Hash, Sha1Hash, Timestamp } from "kachery-js/types/kacheryTypes";
import GarbageMap from "kachery-js/util/GarbageMap";
import fs from 'fs'
import yaml from 'js-yaml'

type FileRecord = {
    sha1: Sha1Hash
    size: ByteCount
    createdTimestamp: Timestamp
    accessedTimestamp: Timestamp
}

class CacheManager {
    #totalSize: ByteCount = byteCount(0)
    #allFiles = new GarbageMap<Sha1Hash, FileRecord>(null)
    constructor(private kacheryStorageManager: KacheryStorageManagerInterface) {
        this.kacheryStorageManager.onFileStored(sha1 => {
            this._getRecordFromFileOnDisk(sha1).then(r => {
                if (r !== undefined) this._updateFileInList(r)
            })
        })
    }
    async initialize() {
        await this._initialize([])
    }
    async _initialize(subdirs: string[]) {
        const storageDir = this.kacheryStorageManager.storageDir()
        let p = `${storageDir}/sha1`
        if (subdirs.length > 0) p = `${p}/${subdirs.join('/')}`
        const a = await fs.promises.readdir(p)
        for (let x of a) {
            if ((subdirs.length <= 2) && (x.length === 2)) {
                await this._initialize([...subdirs, x])
            }
            else if ((subdirs.length === 3) && (isSha1Hash(x)) && (x.startsWith(subdirs.join('')))) {
                const r = await this._getRecordFromFileOnDisk(x)
                if (r) this._updateFileInList(r)
            }
        }
    }
    async _getRecordFromFileOnDisk(sha1: Sha1Hash) {
        //actually look on disk to get the file info
        const storageDir = this.kacheryStorageManager.storageDir()
        const s = sha1
        let p = `${storageDir}/sha1/${s[0]}${s[1]}/${s[2]}${s[3]}/${s[4]}${s[5]}/${s}`
        if (!fs.existsSync(p)) return
        const stat0 = await fs.promises.stat(p)
        const r = {sha1, size: byteCount(stat0.size), createdTimestamp: stat0.ctimeMs as any as Timestamp, accessedTimestamp: stat0.atimeMs as any as Timestamp}
        return r
    }
    async clean() {
        const maxCacheSizeBytes = this._getMaxCacheSizeBytes()
        if (maxCacheSizeBytes) {
            console.info(`Limiting cache size to ${maxCacheSizeBytes/1000000000} GiB.`)
        }
        else {
            console.info('Not limiting cache size.')
        }
        console.info(`Current cache size: ${Number(this.#totalSize)/1000000000} GiB`)
        if (!maxCacheSizeBytes) return

        // if we are below the limit then don't do anything
        if (Number(this.#totalSize) < Number(maxCacheSizeBytes)) return

        // Let's update all the files to make sure we have the right access times (and delete any that are missing)
        await this._updateAllFilesInList()

        console.info(`Current cache size: ${Number(this.#totalSize)/1000000000} GiB`)
        if (!maxCacheSizeBytes) return

        // check again whether we are below the limit
        if (Number(this.#totalSize) < Number(maxCacheSizeBytes)) return

        // Determine the number of bytes we need to clear (ie. free up) - we're going to go down to 70% if capacity
        const numBytesToClear = Number(this.#totalSize) - Number(maxCacheSizeBytes) * 0.7

        console.info(`Cleaning cache: seeking to free ${numBytesToClear} bytes...`)

        // file records sorted by accessed timestamp
        const recordsSortedByAccess = [...this.#allFiles.values()]
        recordsSortedByAccess.sort((r1, r2) => (Number(r1.accessedTimestamp) - Number(r2.accessedTimestamp)))
        const cumulativeSize = cumulativeSum(recordsSortedByAccess.map(r => Number(r.size)))

        // Find the cutoff where we can delete numBytesToClear * 1.3
        let cutoffIndex1 = (cumulativeSize.map((s, i) => ({s, i})).filter(a => (a.s >= numBytesToClear * 1.3))[0] || {}).i
        if (cutoffIndex1 === undefined) cutoffIndex1 = cumulativeSize.length
        const candidateRecords = recordsSortedByAccess.filter((r, i) => (i <= cutoffIndex1))

        // Sorted descending by size - because we want to delete the largest files first
        const candidateRecordsSortedBySize = [...candidateRecords]
        candidateRecordsSortedBySize.sort((r1, r2) => (Number(r2.size) - Number(r1.size)))
        const cumulativeSize2 = cumulativeSum(candidateRecordsSortedBySize.map(r => Number(r.size)))

        // Find the cutoff where we can delete numBytesToClear
        let cutoffIndex2 = (cumulativeSize2.map((s, i) => ({s, i})).filter(a => (a.s >= numBytesToClear))[0] || {}).i
        if (cutoffIndex2 === undefined) cutoffIndex2 = cumulativeSize.length
        const recordsToDelete = candidateRecordsSortedBySize.filter((r, i) => (i <= cutoffIndex2))

        console.info(`Cleaning cache: moving ${recordsToDelete.length} files to trash.`)

        // Now actually move the files to trash
        for (let rec of recordsToDelete) {
            await this._moveFileToTrash(rec.sha1)
        }

        console.info(`Done cleaning cache. Total size is now ${this.#totalSize} bytes.`)
    }
    async _moveFileToTrash(sha1: Sha1Hash) {
        const x = this.#allFiles.get(sha1)
        if (!x) return
        await this.kacheryStorageManager.moveFileToTrash(sha1)
        this._removeFileFromList(sha1)
    }
    _updateFileInList(record: FileRecord) {
        this._removeFileFromList(record.sha1)
        this.#allFiles.set(record.sha1, record)
        this.#totalSize = byteCount(Number(this.#totalSize) + Number(record.size))
    }
    _removeFileFromList(sha1: Sha1Hash) {
        const x = this.#allFiles.get(sha1)
        if (!x) return
        this.#allFiles.delete(sha1)
        this.#totalSize = byteCount(Number(this.#totalSize) - Number(x.size))
    }
    _getMaxCacheSizeBytes() {
        const kacheryYamlPath = `${this.kacheryStorageManager.storageDir()}/kachery.yaml`
        if (!fs.existsSync(kacheryYamlPath)) return undefined
        let yamlContent = fs.readFileSync(kacheryYamlPath, 'utf8');
        let config = yaml.safeLoad(yamlContent)
        if (!isObject(config)) return undefined
        const x = (config as any)['maxCacheSizeGiB']
        if (!x) return undefined
        if (!isNumber(x)) return undefined
        const y = x * 1000 * 1000 * 1000
        if (!isByteCount(y)) return undefined
        return y
    }
    async _updateAllFilesInList() {
        const records = this.#allFiles.values()
        for (let r of records) {
            const r2 = await this._getRecordFromFileOnDisk(r.sha1)
            if (r2) {
                this._updateFileInList(r2)
            }
            else {
                this._removeFileFromList(r.sha1)
            }
        }
    }

}

const cumulativeSum = (x: number[]) => {
    let tot = 0
    return x.map(a => {tot += a; return tot})
}

export default CacheManager