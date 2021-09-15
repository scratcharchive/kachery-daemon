// import { scaledDurationMsec, _validateObject } from "../common/types/kacheryTypes";
import child_process from 'child_process';
import fs from 'fs';
import logger from "winston";;
import { userInfo } from 'os';
import KacheryNode from '../kacheryInterface/core/KacheryNode';
import { elapsedSince, nowTimestamp, scaledDurationMsec } from "../commonInterface/kacheryTypes";
import randomAlphaString from "../commonInterface/util/randomAlphaString";
import { sleepMsec } from '../commonInterface/util/util';

export default class ClientAuthService {
    #node: KacheryNode
    #halted = false
    #currentClientAuthCode = createClientAuthCode()
    constructor(node: KacheryNode, private opts: {clientAuthGroup: string | null}) {
        this.#node = node

        this._start()
    }
    stop() {
        this.#halted = true
    }
    async _start() {
        const intervalMsec = scaledDurationMsec(1000 * 60 * 3)
        // this service should not wait before starting
        while (true) {
            if (this.#halted) return
            
            const previous = this.#currentClientAuthCode
            this.#currentClientAuthCode = createClientAuthCode()
            const clientAuthPath = this.#node.kacheryStorageManager().storageDir() + '/client-auth'
            const clientAuthPathTmp = `${clientAuthPath}.tmp.${randomAlphaString(6)}`
            await fs.promises.writeFile(clientAuthPathTmp, this.#currentClientAuthCode, {mode: fs.constants.S_IRUSR | fs.constants.S_IRGRP | fs.constants.S_IWUSR})
            const group = this.opts.clientAuthGroup
            if (group) {
                const user = userInfo().username
                try {
                    child_process.execSync(`chown ${user}:${group} ${clientAuthPathTmp}`);
                }
                catch(e) {
                    logger.error(`Problem setting ownership of client auth file. Perhaps you do not belong to group "${group}".`, e.message)
                    logger.error('ABORTING')
                    process.exit(1)
                }
            }
            if (fs.existsSync(clientAuthPath)) {
                await fs.promises.unlink(clientAuthPath)
            }
            await renameAndCheck2(clientAuthPathTmp, clientAuthPath, this.#currentClientAuthCode)
            // await fs.promises.rename(clientAuthPathTmp, clientAuthPath)
            this.#node.setClientAuthCode(this.#currentClientAuthCode, previous)

            await sleepMsec(intervalMsec, () => {return !this.#halted})
        }
    }
}

export const renameAndCheck2 = async (srcPath: string, dstPath: string, content: string) => {
    try {
        // this line occassionaly fails on our ceph system and it is unclear the reason. So I am catching the error to troubleshoot
        fs.renameSync(srcPath, dstPath)
    }
    catch(err) {
        if (!fs.existsSync(dstPath)) {
            throw Error(`Unexpected problem renaming file (2). File does not exist: ${dstPath}: ${err.message}`)
        }
        throw Error(`Unexpected problem renaming file (2). Even though file exists: ${dstPath}: ${err.message}`)
    }
    // we need to stat the file here for purpose of flushing to disk (problem encountered on franklab system)
    const timeoutMsec = 1000 * 10
    const timer = nowTimestamp()
    while (true) {
        const content0 = fs.readFileSync(dstPath, 'utf-8')
        if (content0 === content) break // we are good
        await sleepMsec(scaledDurationMsec(100))
        const elapsed = elapsedSince(timer)
        if (elapsed > timeoutMsec) {
            throw Error(`Unexpected: file does not have expected content after renaming (**): ${dstPath} ${content0} ${content}`)
        }
    }
}

const createClientAuthCode = () => {
    return randomAlphaString(12)
}