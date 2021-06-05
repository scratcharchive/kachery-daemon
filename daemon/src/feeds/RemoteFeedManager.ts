import GarbageMap from '../common/GarbageMap';
import { elapsedSince, FeedId, FindLiveFeedResult, scaledDurationMsec, SubfeedHash, Timestamp } from '../common/types/kacheryTypes';
import KacheryDaemonNode from '../KacheryDaemonNode';
import NewOutgoingSubfeedSubscriptionManager from './NewOutgoingSubfeedSubscriptionManager';

class RemoteFeedManager {
    #liveFeedInfos = new GarbageMap<FeedId, {result: FindLiveFeedResult | null, timestamp: Timestamp}>(scaledDurationMsec(60 * 60 * 1000)) // Information about the live feeds (cached in memory) - null result means not found
    // #remoteSubfeedSubscriptions = new GarbageMap<string, RemoteSubfeedSubscription>(null)
    // Manages interactions with feeds on remote nodes within the network
    constructor(private node: KacheryDaemonNode, private outgoingSubfeedSubscriptionManager: NewOutgoingSubfeedSubscriptionManager) {
    }

    async subscribeToRemoteSubfeed(feedId: FeedId, subfeedHash: SubfeedHash): Promise<boolean> {
        // todo: find the node ID and channel of the remote subfeed
        let cachedInfo = this.#liveFeedInfos.get(feedId)
        if (cachedInfo) {
            // invalidate the cached result
            const elapsed = elapsedSince(cachedInfo.timestamp)
            if ((cachedInfo.result) && (elapsed > 1000 * 60 * 10)) {
                cachedInfo = undefined
            }
            else if ((!cachedInfo.result) && (elapsed > 1000 * 30)) {
                cachedInfo = undefined
            }
        }
        if (!cachedInfo) {
            throw Error('kacheryhub todo')
            // const findLiveFeedResult = await this.node.findLiveFeed({feedId, timeoutMsec: TIMEOUTS.defaultRequest})
            // cachedInfo = {result: findLiveFeedResult, timestamp: nowTimestamp()}
            // this.#liveFeedInfos.set(feedId, cachedInfo)
        }
        const findLiveFeedResult = cachedInfo.result
        if (!findLiveFeedResult) return false
        const remoteNodeId = findLiveFeedResult.nodeId
        // CHAIN:get_remote_messages:step(5)
        await this.outgoingSubfeedSubscriptionManager.createOrRenewOutgoingSubscription(remoteNodeId, feedId, subfeedHash)
        return true
    }
}

export default RemoteFeedManager