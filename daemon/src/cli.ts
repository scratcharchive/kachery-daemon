#!/usr/bin/env node

import fs from 'fs';
import os from 'os';
import yargs from 'yargs';
import realExternalInterface from './external/real/realExternalInterface';
import { Address, ChannelLabel, HostName, isAddress, isArrayOf, isBoolean, isChannelLabel, isHostName, isNodeId, isNodeLabel, isPort, isUrlString, LocalFilePath, localFilePath, NodeId, NodeLabel, nodeLabel, optional, toPort, _validateObject } from './common/types/kacheryTypes';
import startDaemon from './startDaemon';

// Thanks: https://stackoverflow.com/questions/4213351/make-node-js-not-exit-on-error
process.on('uncaughtException', function (err) {
  // This is important because utp-native was sporadically giving the following error and crashing:
  console.warn(err.stack);
  console.log('Uncaught exception: ', err);
});

class CLIError extends Error {
  constructor(errorString: string) {
    super(errorString);
  }
}

export interface IsAdmin extends Boolean {
  __isAdmin__: never; // phantom
}
export interface IsMessageProxy extends Boolean {
  __isMessageProxy__: never; // phantom
}
export interface IsDataProxy extends Boolean {
  __isDataProxy__: never; // phantom
}
export interface IsPublic extends Boolean {
  __isPublic__: never; // phantom
}

export interface ChannelConfigAuthorizedNode {
  nodeId: NodeId
  nodeLabel: NodeLabel
  isAdmin?: IsAdmin
  isMessageProxy?: IsMessageProxy
  isDataProxy?: IsDataProxy
  isPublic?: IsPublic
}

export const isChannelConfigAuthorizedNode = (x: any): x is ChannelConfigAuthorizedNode => {
  return _validateObject(x, {
    nodeId: isNodeId,
    nodeLabel: isNodeLabel,
    isAdmin: optional(isBoolean),
    isMessageProxy: optional(isBoolean),
    isDataProxy: optional(isBoolean),
    isPublic: optional(isBoolean)
  }, {allowAdditionalFields: true})
}

export interface ChannelConfig {
  channelLabel: ChannelLabel
  bootstrapAddresses: Address[]
  authorizedNodes: ChannelConfigAuthorizedNode[]
}

export const isChannelConfig = (x: any): x is ChannelConfig => {
  return _validateObject(x, {
    channelLabel: isChannelLabel,
    bootstrapAddresses: isArrayOf(isAddress),
    authorizedNodes: isArrayOf(isChannelConfigAuthorizedNode)
  }, {allowAdditionalFields: true})
}

function main() {
  const argv = yargs
    .scriptName('kachery-daemon-node')
    .command({
      command: 'start',
      describe: 'Start the daemon',
      builder: (y) => {
        y.option('verbose', {
          describe: 'Verbosity level.',
          type: 'number',
          default: 0
        })
        y.option('label', {
          describe: 'Label for this node (required).',
          type: 'string'
        })
        y.option('owner', {
          describe: 'Owner ID for this node (optional).',
          type: 'string',
          default: ''
        })
        y.option('auth-group', {
          describe: 'The os group that has access to this daemon',
          type: 'string'
        })
        return y
      },
      handler: async (argv) => {
        const hostName = argv.host || null;
        const daemonApiPort = Number(process.env.KACHERY_DAEMON_PORT || 20431)
        const label = nodeLabel(argv.label as string)
        const ownerId = argv.owner as string
        
        const verbose = Number(argv.verbose || 0)
        const authGroup: string | null = argv['auth-group'] ? argv['auth-group'] + '' : null 

        if (!isPort(daemonApiPort)) {
          throw new CLIError(`Invalid daemon api port: ${daemonApiPort}`);
        }

        let storageDir = process.env['KACHERY_STORAGE_DIR'] || ''
        if (!storageDir) {
          storageDir = `${os.homedir()}/kachery-storage`
            console.warn(`Using ${storageDir} for storage. Set KACHERY_STORAGE_DIR to override.`);
            if (!fs.existsSync(storageDir)) {
              fs.mkdirSync(storageDir)
            }
        }
        if ((!fs.lstatSync(storageDir).isDirectory()) && (!fs.lstatSync(storageDir).isSymbolicLink)) {
          throw new CLIError(`Storage path is not a directory: ${storageDir}`)
        }        

        const externalInterface = realExternalInterface(localFilePath(storageDir))

        startDaemon({
          verbose,
          daemonApiPort,
          label,
          ownerId,
          externalInterface,
          opts: {
            authGroup,
            services: {
                display: true,
                daemonServer: true,
                mirror: true,
                kacheryHub: true,
                clientAuth: true
            }
          }
        })
      }
    })
    .demandCommand()
    .strict()
    .help()
    .wrap(72)
    .argv
}

main();