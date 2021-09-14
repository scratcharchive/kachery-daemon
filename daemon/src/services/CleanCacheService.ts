import logger from "winston";
import CacheManager from '../cacheManager/CacheManager'
import { KacheryNode } from '../kachery-js'
import { ByteCount, scaledDurationMsec } from "../kachery-js/types/kacheryTypes"
import { sleepMsec } from "../kachery-js/util/util"
import { action } from "./action"

export default class CleanCacheService {
    #halted = false
    #cacheManager: CacheManager
    constructor(private node: KacheryNode, private opts: {}) {
        this.#cacheManager = new CacheManager(this.node.kacheryStorageManager())

        this._start()
    }
    stop() {
        this.#halted = true
    }
    async _start() {
        const intervalMsec = scaledDurationMsec(1000 * 60 * 3)
        // wait a bit before starting
        await sleepMsec(scaledDurationMsec(1000 * 1), () => {return !this.#halted})
        /////////////////////////////////////////////////////////////////////////
        await action('initialize-clean-cache', {}, async () => {
            await this.#cacheManager.initialize()
        }, async (err: Error) => {
            logger.error(`Problem initializing clean-cache (${err.message})`)
        });
        /////////////////////////////////////////////////////////////////////////
        while (true) {
            if (this.#halted) return
            /////////////////////////////////////////////////////////////////////////
            await action('clean-cache', {}, async () => {
                await this.#cacheManager.clean()
            }, async (err: Error) => {
                logger.error(`Problem cleaning cache (${err.message})`)
            });
            /////////////////////////////////////////////////////////////////////////

            await sleepMsec(intervalMsec, () => {return !this.#halted})
        }
    }
}