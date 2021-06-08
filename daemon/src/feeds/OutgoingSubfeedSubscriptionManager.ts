import GarbageMap from "../common/GarbageMap"
import { DurationMsec, durationMsecToNumber, elapsedSince, FeedId, nowTimestamp, scaledDurationMsec, SubfeedHash, zeroTimestamp } from "../common/types/kacheryTypes"

class OutgoingSubfeedSubscriptionManager {
    #outgoingSubscriptions = new GarbageMap<string, OutgoingSubfeedSubscription>(scaledDurationMsec(300 * 60 * 1000))
    #subscribeToRemoteSubfeedCallbacks: ((feedId: FeedId, subfeedHash: SubfeedHash) => void)[] = []
    constructor() {
    }
    async createOrRenewOutgoingSubscription(feedId: FeedId, subfeedHash: SubfeedHash): Promise<void> {
        const subfeedCode = makeSubscriptionCode(feedId, subfeedHash)
        let S = this.#outgoingSubscriptions.get(subfeedCode)
        if (!S) {
            S = new OutgoingSubfeedSubscription(feedId, subfeedHash)
            this.#outgoingSubscriptions.set(subfeedCode, S)
            S.onSubscribeToRemoteSubfeed((feedId: FeedId, subfeedHash: SubfeedHash) => {
                this.#subscribeToRemoteSubfeedCallbacks.forEach(cb => {
                    cb(feedId, subfeedHash)
                })
            })
        }
        await S.renew()
        setTimeout(() => {
            this._checkRemove(feedId, subfeedHash)
        }, durationMsecToNumber(S.durationMsec()) +  durationMsecToNumber(scaledDurationMsec(5000)))
    }
    onSubscribeToRemoteSubfeed(callback: (feedId: FeedId, subfeedHash: SubfeedHash) => void) {
        this.#subscribeToRemoteSubfeedCallbacks.push(callback)
    }
    hasSubfeedSubscription(feedId: FeedId, subfeedHash: SubfeedHash) {
        const subfeedCode = makeSubscriptionCode(feedId, subfeedHash)
        return this.#outgoingSubscriptions.has(subfeedCode)
    }
    _checkRemove(feedId: FeedId, subfeedHash: SubfeedHash) {
        const subfeedCode = makeSubscriptionCode(feedId, subfeedHash)
        const S = this.#outgoingSubscriptions.get(subfeedCode)
        if (!S) return
        const elapsedMsec = S.elapsedMsecSinceLastRenew()
        if (elapsedMsec > durationMsecToNumber(S.durationMsec())) {
            this.#outgoingSubscriptions.delete(subfeedCode)
        }
    }
}

const makeSubscriptionCode = (feedId: FeedId, subfeedHash: SubfeedHash) => {
    return feedId.toString() + ':' + subfeedHash.toString()
}

class OutgoingSubfeedSubscription {
    #lastRenewTimestamp = zeroTimestamp()
    #lastRenewDurationMsec: DurationMsec = scaledDurationMsec(1000 * 60)
    #subscribeToRemoteSubfeedCallbacks: ((feedId: FeedId, subfeedHash: SubfeedHash) => void)[] = []
    constructor(private feedId: FeedId, private subfeedHash: SubfeedHash) {
    }
    async renew(): Promise<void> {
        if (elapsedSince(this.#lastRenewTimestamp) < durationMsecToNumber(this.durationMsec()) / 2) {
            return
        }
        this.#lastRenewTimestamp = nowTimestamp()
        this.#subscribeToRemoteSubfeedCallbacks.forEach(cb => {
            cb(this.feedId, this.subfeedHash)
        })
    }
    elapsedMsecSinceLastRenew() {
        return elapsedSince(this.#lastRenewTimestamp)
    }
    durationMsec() {
        return this.#lastRenewDurationMsec
    }
    onSubscribeToRemoteSubfeed(callback: (feedId: FeedId, subfeedHash: SubfeedHash) => void) {
        this.#subscribeToRemoteSubfeedCallbacks.push(callback)
    }
}

export default OutgoingSubfeedSubscriptionManager