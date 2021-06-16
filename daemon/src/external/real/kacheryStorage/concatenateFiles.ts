import crypto from 'crypto'
import fs from 'fs'
import { localFilePath, LocalFilePath, Sha1Hash } from '../../../kachery-js/types/kacheryTypes'
import randomAlphaString from '../../../kachery-js/util/randomAlphaString'

const _getTemporaryDirectory = (storageDir: LocalFilePath) => {
    const ret = storageDir + '/tmp'
    mkdirIfNeeded(localFilePath(ret))
    return ret
}

const mkdirIfNeeded = (path: LocalFilePath) => {
    if (!fs.existsSync(path.toString())) {
        try {
            fs.mkdirSync(path.toString())
        }
        catch(err) {
            if (!fs.existsSync(path.toString())) {
                fs.mkdirSync(path.toString())
            }
        }
    }
}

export const createTemporaryFilePath = (args: {storageDir: LocalFilePath, prefix: string}) => {
    const dirPath = _getTemporaryDirectory(args.storageDir)
    return `${dirPath}/${args.prefix}-${randomAlphaString(10)}`
}