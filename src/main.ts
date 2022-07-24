import config from 'config';
import { queueBroker } from './utils/queueBroker';
import { log } from './utils/logger';

import {
  arweaveTxVerifierFactory,
  chunkIdVerifierFactory,
} from './arweaveVerifier';
import { VERIFY_ARWEAVE_TX, VERIFY_CHUNK_ID } from './utils/queueNames';

export async function main() {
  log.info('Subscribing to verify tx and chunkId queue');
  // await queueBroker.subscribeDelayed(VERIFY_ARWEAVE_TX, {
  //   handlerFactory: arweaveTxVerifierFactory,
  //   maxConcurrency: config.get('arweave_tx_verifier.max_concurrency'),
  // });
  await queueBroker.subscribeDelayed(VERIFY_CHUNK_ID, {
    handlerFactory: chunkIdVerifierFactory,
    maxConcurrency: config.get('chunk_id_verifier.max_concurrency'),
  });
}
