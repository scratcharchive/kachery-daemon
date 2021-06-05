import { MessageCount, messageCountToNumber, NodeId, SignedSubfeedMessage } from "../common/types/kacheryTypes";
import KacheryDaemonNode from "../KacheryDaemonNode";
import Subfeed from "./Subfeed";

class RemoteSubfeedMessageDownloader {
    #remoteNodeId: NodeId | null = null
    #numRemoteMessages: MessageCount | null = null
    #activeDownload = false
    constructor(private node: KacheryDaemonNode, private subfeed: Subfeed) {
    }
    reportNumRemoteMessages(remoteNodeId: NodeId, numRemoteMessages: MessageCount) {
        this.#remoteNodeId = remoteNodeId
        this.#numRemoteMessages = numRemoteMessages
        if (this.subfeed.getNumLocalMessages() < this.#numRemoteMessages) {
            if (!this.#activeDownload) this._startDownload()
        }
    }
    async _startDownload() {
        if (this.#activeDownload) return
        this.#activeDownload = true
        while (true) {
            const numLocalMessages = this.subfeed.getNumLocalMessages()
            const numRemoteMessages = this.#numRemoteMessages
            if (numRemoteMessages === null) break
            const remoteNodeId = this.#remoteNodeId
            if (remoteNodeId === null) break
            const numMessagesToDownload = messageCountToNumber(numRemoteMessages) - messageCountToNumber(numLocalMessages)
            if (numMessagesToDownload <= 0) break
            let signedMessages: SignedSubfeedMessage[]
            throw Error('kacheryhub todo')
            // try {
            //     signedMessages = await this._doDownloadRemoteMessages(remoteNodeId, subfeedPosition(messageCountToNumber(numLocalMessages)), messageCount(numMessagesToDownload))
            // }
            // catch(err) {
            //     console.warn('Error downloading remote subfeed messages', err)
            //     break
            // }
            if (signedMessages.length !== numMessagesToDownload) {
                console.warn('Unexpected problem downloading remote subfeed messages. Got unexpected number of messages.')
                break
            }
            // maybe somehow the number of local messages has changed (shouldn't happen though)
            const signedMessagesToAppend = signedMessages.slice(messageCountToNumber(this.subfeed.getNumLocalMessages()) - messageCountToNumber(numLocalMessages))
            await this.subfeed.appendSignedMessages(signedMessagesToAppend)
        }
        this.#activeDownload = false
    }
}

export default RemoteSubfeedMessageDownloader