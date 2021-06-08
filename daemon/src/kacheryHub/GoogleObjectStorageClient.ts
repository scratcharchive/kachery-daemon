import axios from "axios"
import { JSONValue } from "../common/types/kacheryTypes"

export type GoogleObjectStorageClientOpts = {
    bucketName: string
}

class GoogleObjectStorageClient {
    constructor(private opts: GoogleObjectStorageClientOpts) {
    }
    async getObjectData(name: string, opts: {cacheBust?: boolean} = {}): Promise<ArrayBuffer | null> {
        let url = `https://storage.googleapis.com/${this.opts.bucketName}/${name}`
        if (opts.cacheBust) {
            url = cacheBust(url)
        }
        let resp = null
        try {
            resp = await axios.get(url, {responseType: 'arraybuffer'})
        }
        catch(err) {
            return null
        }
        if ((resp) && (resp.data)) {
            return resp.data
        }
        else return null
    }
    async getObjectJson(name: string, opts: {cacheBust?: boolean} = {}): Promise<JSONValue | null> {
        const data = await this.getObjectData(name, opts)
        if (!data) return null
        let ret: JSONValue
        try {
            ret = JSON.parse(new TextDecoder().decode(data)) as any as JSONValue
        }
        catch(err) {
            console.warn(`Problem parsing JSON for object: ${name}`)
            return null
        }
        return ret
    }
}

const cacheBust = (url: string) => {
    if (url.includes('?')) {
        return url + `&cb=${randomAlphaString(10)}`
    }
    else {
        return url + `?cb=${randomAlphaString(10)}`
    }
}

export const randomAlphaString = (num_chars: number) => {
    if (!num_chars) {
        /* istanbul ignore next */
        throw Error('randomAlphaString: num_chars needs to be a positive integer.')
    }
    var text = "";
    var possible = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
    for (var i = 0; i < num_chars; i++)
        text += possible.charAt(Math.floor(Math.random() * possible.length));
    return text;
}

export default GoogleObjectStorageClient