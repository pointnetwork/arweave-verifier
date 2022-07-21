import { queueBroker } from './utils/queueBroker';
import { log } from './utils/logger';
import { arweaveTxVerifier, chunkIdVerifier } from './arweaveVerifier';
import { VERIFY_ARWEAVE_TX, VERIFY_CHUNK_ID } from './utils/queueNames';

export async function main() {
  log.info('Subscribing to verify tx and chunkId queue');
  await queueBroker.subscribeDelayed(VERIFY_ARWEAVE_TX, arweaveTxVerifier);
  await queueBroker.subscribeDelayed(VERIFY_CHUNK_ID, chunkIdVerifier);
}
