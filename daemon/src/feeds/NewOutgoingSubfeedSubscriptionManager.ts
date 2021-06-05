import GarbageMap from "../common/GarbageMap"
import { DurationMsec, durationMsecToNumber, elapsedSince, FeedId, NodeId, nowTimestamp, scaledDurationMsec, SubfeedHash, zeroTimestamp } from "../common/types/kacheryTypes"
import KacheryDaemonNode from "../KacheryDaemonNode"

class NewOutgoingSubfeedSubscriptionManager {
    #outgoingSubscriptions = new GarbageMap<string, OutgoingSubfeedSubscription>(scaledDurationMsec(300 * 60 * 1000))
    constructor(private node: KacheryDaemonNode) {
    }
    async createOrRenewOutgoingSubscription(remoteNodeId: NodeId, feedId: FeedId, subfeedHash: SubfeedHash): Promise<void> {
        const subfeedCode = makeSubscriptionCode(remoteNodeId, feedId, subfeedHash)
        let S = this.#outgoingSubscriptions.get(subfeedCode)
        if (!S) {
            S = new OutgoingSubfeedSubscription(this.node, remoteNodeId, feedId, subfeedHash)
            this.#outgoingSubscriptions.set(subfeedCode, S)
        }
        // CHAIN:get_remote_messages:step(6)
        await S.renew()
        setTimeout(() => {
            this._checkRemove(remoteNodeId, feedId, subfeedHash)
        }, durationMsecToNumber(S.durationMsec()) +  durationMsecToNumber(scaledDurationMsec(5000)))
    }
    _checkRemove(remoteNodeId: NodeId, feedId: FeedId, subfeedHash: SubfeedHash) {
        const subfeedCode = makeSubscriptionCode(remoteNodeId, feedId, subfeedHash)
        const S = this.#outgoingSubscriptions.get(subfeedCode)
        if (!S) return
        const elapsedMsec = S.elapsedMsecSinceLastRenew()
        if (elapsedMsec > durationMsecToNumber(S.durationMsec())) {
            this.#outgoingSubscriptions.delete(subfeedCode)
        }
    }
}

const makeSubscriptionCode = (remoteNodeId: NodeId, feedId: FeedId, subfeedHash: SubfeedHash) => {
    return remoteNodeId + ':' + feedId.toString() + ':' + subfeedHash.toString()
}

class OutgoingSubfeedSubscription {
    #lastRenewTimestamp = zeroTimestamp()
    #lastRenewDurationMsec: DurationMsec = scaledDurationMsec(1000 * 60)
    #initialMessageSent = false
    constructor(private node: KacheryDaemonNode, private remoteNodeId: NodeId, private feedId: FeedId, private subfeedHash: SubfeedHash) {
    }
    async renew(): Promise<void> {
        if (elapsedSince(this.#lastRenewTimestamp) < durationMsecToNumber(this.durationMsec()) / 2) {
            return
        }
        this.#lastRenewTimestamp = nowTimestamp()
        throw Error('kacheryhub todo')
        // // CHAIN:get_remote_messages:step(7)
        // const requestData: SubscribeToSubfeedRequestData = {
        //     requestType: 'subscribeToSubfeed',
        //     feedId: this.feedId,
        //     subfeedHash: this.subfeedHash
        // }
        // const responseData = await this.node.remoteNodeManager().sendRequestToNode(
        //     this.remoteNodeId,
        //     requestData,
        //     {
        //         timeoutMsec: TIMEOUTS.defaultRequest,
        //         method: 'default'
        //     }
        // )
        // if (!isSubscribeToSubfeedResponseData(responseData)) throw Error('Unexpected response to subscribeToSubfeed')
        // if (!responseData.success) throw Error(`Error in response to subscribeToSubfeed: ${responseData.errorMessage}`)
        // const numRemoteMessages = responseData.numMessages
        // if (numRemoteMessages !== null) {
        //     this.node.feedManager().reportNumRemoteMessages(this.remoteNodeId, this.feedId, this.subfeedHash, numRemoteMessages)
        // }
    }
    elapsedMsecSinceLastRenew() {
        return elapsedSince(this.#lastRenewTimestamp)
    }
    durationMsec() {
        return this.#lastRenewDurationMsec
    }
}

export default NewOutgoingSubfeedSubscriptionManager