import { queueBroker } from './utils/queueBroker';
import { log } from './utils/logger';
import { arweaveTxVerifier, chunkIdVerifier } from './arweaveVerifier';

export async function main() {
  log.info('Subscribing to upload queue');
  await queueBroker.subscribe('verifyArweaveTx', arweaveTxVerifier);
  await queueBroker.subscribe('verifyChunkId', chunkIdVerifier);
}
