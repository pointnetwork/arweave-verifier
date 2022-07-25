import config from 'config';
import axios from 'axios';
import { queueBroker } from './utils/queueBroker';
import { log } from './utils/logger';

import {
  arweaveTxVerifierFactory,
  chunkIdVerifierFactory,
} from './arweaveVerifier';
import { VERIFY_ARWEAVE_TX, VERIFY_CHUNK_ID } from './utils/queueNames';
import { safeStringify } from './utils/safeStringify';
import { fromMinutesToMilliseconds } from './utils/fromMinutesToMilliseconds';

export async function main() {
  log.info('Subscribing to verify tx and chunkId queue');
  await queueBroker.subscribeDelayed(VERIFY_ARWEAVE_TX, {
    handlerFactory: arweaveTxVerifierFactory,
    maxConcurrency: config.get('arweave_tx_verifier.max_concurrency'),
  });
  await queueBroker.subscribeDelayed(VERIFY_CHUNK_ID, {
    handlerFactory: chunkIdVerifierFactory,
    maxConcurrency: config.get('chunk_id_verifier.max_concurrency'),
  });
}

process.on('uncaughtException', function (error: any) {
  if (error?.code !== 'ENETDOWN') {
    log.error(
      'Logger error connection has failed. It will not exit the process'
    );
    process.exit(1);
  } else {
    log.error(`Uncaught error: ${safeStringify(error)}`);
    process.exit(1);
  }
});

function keepAppAlive() {
  const appName = process.env.HEROKU_APP_NAME;
  if (appName) {
    const interval = fromMinutesToMilliseconds(
      config.get('keep_alive_interval')
    );
    const url = `https://${appName}.herokuapp.com/health`;
    setTimeout(async () => {
      try {
        await axios.get(url);
      } catch {
        // do nothing, it will try again
      }
    }, interval);
    keepAppAlive();
  }
}

keepAppAlive();
