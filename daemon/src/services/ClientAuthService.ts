// import { scaledDurationMsec, _validateObject } from "../common/types/kacheryTypes";
import { scaledDurationMsec, _validateObject } from "../kachery-js/types/kacheryTypes"
import { sleepMsec } from '../kachery-js/util/util'
import { KacheryNode } from '../kachery-js';
import child_process from 'child_process'
import fs from 'fs'
import { userInfo } from 'os'
import randomAlphaString from "../kachery-js/util/randomAlphaString";

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
            const clientAuthPathTmp = clientAuthPath + '.tmp'
            await fs.promises.writeFile(clientAuthPathTmp, this.#currentClientAuthCode, {mode: fs.constants.S_IRUSR | fs.constants.S_IRGRP | fs.constants.S_IWUSR})
            const group = this.opts.clientAuthGroup
            if (group) {
                const user = userInfo().username
                try {
                    child_process.execSync(`chown ${user}:${group} ${clientAuthPathTmp}`);
                }
                catch(e) {
                    console.warn(`Problem setting ownership of client auth file. Perhaps you do not belong to group "${group}".`, e.message)
                    console.warn('ABORTING')
                    process.exit(1)
                }
            }
            if (fs.existsSync(clientAuthPath)) {
                await fs.promises.unlink(clientAuthPath)
            }
            await fs.promises.rename(clientAuthPathTmp, clientAuthPath)
            this.#node.setClientAuthCode(this.#currentClientAuthCode, previous)

            await sleepMsec(intervalMsec, () => {return !this.#halted})
        }
    }
}

const createClientAuthCode = () => {
    return randomAlphaString(12)
}