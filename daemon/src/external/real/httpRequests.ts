import axios from 'axios';
import { ClientRequest } from 'http';
import { NodeStats } from 'kachery-js';
import { Address, byteCount, ByteCount, ChannelName, DurationMsec, durationMsecToNumber, JSONObject, UrlPath, urlString, UrlString } from 'kachery-js/types/kacheryTypes';
import DataStreamy from 'kachery-js/util/DataStreamy';
import { Socket } from 'net';

export const _tests: {[key: string]: () => Promise<void>} = {}

export class HttpPostJsonError extends Error {
    constructor(errorString: string) {
        super(errorString);
    }
}

const formUrl = (address: Address, path: UrlPath): UrlString => {
    let url: UrlString
    if (address.url) {
        url = urlString(address.url.toString() + path)
    }
    else if ((address.hostName) && (address.port)) {
        url = urlString('http://' + address.hostName + ':' + address.port + path)
    }
    else {
        throw Error(`Unexpected address in formUrl: ${address}`)
    }
    return url
}

export const httpPostJson = async (address: Address, path: UrlPath, data: Object, opts: {timeoutMsec: DurationMsec}): Promise<JSONObject> => {
    const url = formUrl(address, path)
    let res
    try {
        res = await axios.post(url.toString(), data, {timeout: durationMsecToNumber(opts.timeoutMsec), responseType: 'json'})
    }
    catch(err) {
        throw new HttpPostJsonError(err.message)
    }
    return res.data
}
export const httpGetDownload = async (address: Address, path: UrlPath, stats: NodeStats, channelName: ChannelName | null): Promise<DataStreamy> => {
    const url = formUrl(address, path)
    return await httpUrlDownload(url, stats, channelName)
}

export const httpUrlDownload = async (url: UrlString, stats: NodeStats, channelName: ChannelName | null): Promise<DataStreamy> => {
    const res = await axios.get(url.toString(), {responseType: 'stream'})
    const stream = res.data
    const socket: Socket = stream.socket
    const req: ClientRequest = stream.req
    const size: ByteCount = res.headers['Content-Length']
    const ret = new DataStreamy()
    let complete = false
    ret.producer().start(size)
    ret.producer().onCancelled(() => {
        if (complete) return
        // todo: is this the right way to close it?
        req.destroy()
    })
    stream.on('data', (data: Buffer) => {
        if (complete) return
        stats.reportBytesReceived(byteCount(data.length), channelName)
        ret.producer().data(data)
    })
    stream.on('error', (err: Error) => {
        if (complete) return
        complete = true
        ret.producer().error(err)
    })
    stream.on('end', () => {
        if (complete) return
        complete = true
        ret.producer().end()
    })
    socket.on('close', () => {
        if (complete) return
        complete = true
        ret.producer().error(Error('Socket closed.'))
    })

    return ret
}