import { MessageCount, SignedSubfeedMessage } from "../common/types/kacheryTypes";
import KacheryDaemonNode from "../KacheryDaemonNode";
import KacheryHubInterface from "../kacheryHub/KacheryHubInterface";
import Subfeed from "./Subfeed";

class RemoteSubfeedMessageDownloader {
    constructor(private kacheryHubInterface: KacheryHubInterface, private subfeed: Subfeed) {

    }
    async reportNumRemoteMessages(channelName: string, numRemoteMessages: MessageCount) {
        const feedId = this.subfeed.getFeedId()
        const subfeedHash = this.subfeed.getSubfeedHash()
        const numLocalMessages = this.subfeed.getNumLocalMessages()
        if (numRemoteMessages > numLocalMessages) {
            const signedMessages: SignedSubfeedMessage[] = await this.kacheryHubInterface.downloadSignedSubfeedMessages(
                channelName,
                feedId,
                subfeedHash,
                numLocalMessages,
                numRemoteMessages
            )
            // need to check once again the number of local messages (it might have channges)
            let i = this.subfeed.getNumLocalMessages()
            if ((i >= numLocalMessages) && (i < numRemoteMessages)) {
                await this.subfeed.appendSignedMessages(signedMessages.slice(Number(i) - Number(numLocalMessages)))
            }
        }
    }
}

export default RemoteSubfeedMessageDownloader