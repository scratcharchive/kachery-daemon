import fs from 'fs'
import logger from "winston";
import os from 'os'

export const getStorageDir = () => {
    let storageDir = process.env['KACHERY_STORAGE_DIR'] || ''
    if (!storageDir) {
        storageDir = `${os.homedir()}/kachery-storage`
        // can't use logger.warn here because logger uses getStorageDir
        console.warn(`Using ${storageDir} for storage. Set KACHERY_STORAGE_DIR to override.`);
        if (!fs.existsSync(storageDir)) {
            fs.mkdirSync(storageDir)
        }
    }
    return storageDir
}