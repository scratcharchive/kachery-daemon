import ExternalInterface from './external/ExternalInterface';
import { NodeLabel, Port, UserId } from './common/types/kacheryTypes';
import KacheryDaemonNode from './KacheryDaemonNode';
import ClientAuthService from './services/ClientAuthService';
import DaemonApiServer from './services/DaemonApiServer';
import DisplayStateService from './services/DisplayStateService';
import KacheryHubService from './services/KacheryHubService';

export interface StartDaemonOpts {
    authGroup: string | null,
    services: {
        display?: boolean,
        daemonServer?: boolean
        mirror?: boolean,
        kacheryHub?: boolean,
        clientAuth?: boolean
    },
    kacheryHubUrl: string
}

export interface DaemonInterface {
    daemonApiServer: DaemonApiServer | null,
    displayService: DisplayStateService | null,
    kacheryHubService: KacheryHubService | null,
    clientAuthService: ClientAuthService | null,
    node: KacheryDaemonNode,
    stop: () => void
}

const startDaemon = async (args: {
    verbose: number,
    daemonApiPort: Port | null,
    label: NodeLabel,
    ownerId?: UserId,
    externalInterface: ExternalInterface,
    opts: StartDaemonOpts
}): Promise<DaemonInterface> => {
    const {
        verbose,
        daemonApiPort,
        label,
        ownerId,
        externalInterface,
        opts
    } = args
    const kNode = new KacheryDaemonNode({
        verbose,
        label,
        ownerId,
        externalInterface,
        opts: {
            kacheryHubUrl: opts.kacheryHubUrl
        }
    })

    // Start the daemon http server
    const daemonApiServer = new DaemonApiServer(kNode, { verbose });
    if (opts.services.daemonServer && (daemonApiPort !== null)) {
        await daemonApiServer.listen(daemonApiPort);
        console.info(`Daemon http server listening on port ${daemonApiPort}`)
    }

    // start the other services
    let displayService = opts.services.display ? new DisplayStateService(kNode, {
        daemonApiPort
    }) : null
    const kacheryHubService = opts.services.kacheryHub ? new KacheryHubService(kNode, {
    }): null
    const clientAuthService = opts.services.clientAuth ? new ClientAuthService(kNode, {
        clientAuthGroup: opts.authGroup ? opts.authGroup : null
    }) : null

    const _stop = () => {
        displayService && displayService.stop()
        kacheryHubService && kacheryHubService.stop()
        clientAuthService && clientAuthService.stop()
        // wait a bit after stopping services before cleaning up the rest (for clean exit of services)
        setTimeout(() => {
            daemonApiServer && daemonApiServer.stop()
            setTimeout(() => {
                kNode.cleanup()
            }, 20)
        }, 20)
    }

    return {
        daemonApiServer,
        displayService,
        kacheryHubService,
        clientAuthService,
        node: kNode,
        stop: _stop
    }
}

export default startDaemon