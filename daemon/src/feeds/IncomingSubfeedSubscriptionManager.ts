import GarbageMap from "../common/GarbageMap";
import { durationMsecToNumber, elapsedSince, FeedId, MessageCount, nowTimestamp, scaledDurationMsec, SubfeedHash, zeroTimestamp } from "../common/types/kacheryTypes";
import KacheryDaemonNode from "../KacheryDaemonNode";

class IncomingSubfeedSubscriptionManager {
    #incomingSubscriptions = new GarbageMap<string, IncomingSubfeedSubscription>(scaledDurationMsec(300 * 60 * 1000))
    #subscriptionCodesBySubfeedCode = new GarbageMap<string, { [key: string]: boolean }>(scaledDurationMsec(300 * 60 * 1000))
    // #reportToChannelMessagesAddedCallbacks: ((channelName: string, feedId: FeedId, subfeedHash: SubfeedHash, numMessages: MessageCount) => void)[]
    constructor() {

    }
    createOrRenewIncomingSubscription(channelName: string, feedId: FeedId, subfeedHash: SubfeedHash) {
        const subscriptionCode = makeSubscriptionCode(channelName, feedId, subfeedHash)
        const subfeedCode = makeSubfeedCode(feedId, subfeedHash)
        this.#subscriptionCodesBySubfeedCode.set(subfeedCode, {...this.#subscriptionCodesBySubfeedCode.get(subfeedCode) || {}, [subscriptionCode]: true})
        let S = this.#incomingSubscriptions.get(subfeedCode)
        if (!S) {
            S = new IncomingSubfeedSubscription(channelName, feedId, subfeedHash)
            this.#incomingSubscriptions.set(subscriptionCode, S)
        }
        S.renew()
        setTimeout(() => {
            this._checkRemove(channelName, feedId, subfeedHash)
        }, durationMsecToNumber(scaledDurationMsec(60000)))
    }
    getChannelsSubscribingToSubfeed(feedId: FeedId, subfeedHash: SubfeedHash) {
        const subfeedCode = makeSubfeedCode(feedId, subfeedHash)
        const x = this.#subscriptionCodesBySubfeedCode.get(subfeedCode) || {}
        const ret: string[] = []
        for (let subscriptionCode in x) {
            const s = this.#incomingSubscriptions.get(subscriptionCode)
            if (s) {
                ret.push(s.channelName)
            }
        }
        return ret
    }
    // reportMessagesAdded(feedId: FeedId, subfeedHash: SubfeedHash, numMessages: MessageCount) {
    //     const subfeedCode = makeSubfeedCode(feedId, subfeedHash)
    //     const x = this.#subscriptionCodesBySubfeedCode.get(subfeedCode) || {}
    //     for (let subscriptionCode in x) {
    //         const s = this.#incomingSubscriptions.get(subscriptionCode)
    //         if (s) {
    //             this.#reportToChannelMessagesAddedCallbacks.forEach(cb => {
    //                 cb(s.channelName, feedId, subfeedHash, numMessages)
    //             })
    //         }
    //     }
    // }
    // onReportToChannelMessagesAdded(callback: (channelName: string, feedId: FeedId, subfeedHash: SubfeedHash, numMessages: MessageCount) => void) {
    //     this.#reportToChannelMessagesAddedCallbacks.push(callback)
    // }
    _checkRemove(channelName: string, feedId: FeedId, subfeedHash: SubfeedHash) {
        const subfeedCode = makeSubscriptionCode(channelName, feedId, subfeedHash)
        const S = this.#incomingSubscriptions.get(subfeedCode)
        if (!S) return
        const elapsedMsec = S.elapsedMsecSinceLastRenew()
        if (elapsedMsec > durationMsecToNumber(S.durationMsec())) {
            this.#incomingSubscriptions.delete(subfeedCode)
        }
    }
}

class IncomingSubfeedSubscription {
    #lastRenewTimestamp = zeroTimestamp()
    constructor(public channelName: string, public feedId: FeedId, public subfeedHash: SubfeedHash) {
    }
    renew() {
        this.#lastRenewTimestamp = nowTimestamp()
    }
    durationMsec() {
        return scaledDurationMsec(60 * 1000)
    }
    elapsedMsecSinceLastRenew() {
        return elapsedSince(this.#lastRenewTimestamp)
    }
}

const makeSubscriptionCode = (channelName: string, feedId: FeedId, subfeedHash: SubfeedHash) => {
    return channelName + ':' + feedId.toString() + ':' + subfeedHash.toString()
}

const makeSubfeedCode = (feedId: FeedId, subfeedHash: SubfeedHash) => {
    return feedId.toString() + ':' + subfeedHash.toString()
}

export default IncomingSubfeedSubscriptionManager