import { request as gqlRequest } from 'graphql-request';
import config from 'config';
import axios from 'axios';
import { getContent } from './utils/getContent';
import { queueBroker } from './utils/queueBroker';
import getDownloadQuery from './utils/getDownloadQuery';
import { hashFn } from './utils/hashFn';
import { delay } from './utils/delay';
import { log } from './utils/logger';
import { safeStringify } from './utils/safeStringify';
import { getStatus } from './arweaveTransport';

const VERIFY_INTERVAL = 60 * 5; // 5 minutes

export async function downloadFileUrl(fileUrl: string) {
  const fileStream = await axios.get(fileUrl, { responseType: 'stream' });
  return new Promise(async (resolve) => {
    const data: any = [];
    (fileStream.data as any)
      .on('data', (chunk: any) => {
        data.push(chunk);
      })
      .on('end', () => {
        const buffer = Buffer.concat(data);
        resolve(buffer);
      });
  });
}

const GATEWAY_URL: string = config.get('storage.arweave_gateway_url');

async function getTxIdForChunkId(chunkId: string): Promise<string | undefined> {
  const downloadQuery = getDownloadQuery(chunkId, 'desc');
  const queryResult: any = await gqlRequest(GATEWAY_URL, downloadQuery);
  for (const edgeResult of queryResult.transactions.edges) {
    const {
      node: { id },
    } = edgeResult;
    const file = (await downloadFileUrl(`https://arweave.net/${id}`)) as Buffer;
    const fileHash = hashFn(file).toString('hex');
    if (fileHash === chunkId) {
      return id;
    }
  }
}

export async function chunkIdVerifier(msg) {
  const content = getContent(msg.content);
  const { chunkId } = content;
  try {
    log.info('Cheking if chunkId has been uploaded to arweave and is valid');
    const arweaveTxId = await getTxIdForChunkId(chunkId);
    if (!arweaveTxId) {
      await queueBroker.sendMessage('upload', { ...content });
      log.info(
        'ChunkId not found in arweave, send message to arweaveUploader'
      );
    } else {
      log.info(
        `ChunkId found in arweave tx: ${arweaveTxId}, check tx status and we are done`
      );
      await queueBroker.sendMessage('verifyArweaveTx', {
        ...content,
        txid: arweaveTxId,
      });
    }
    await queueBroker.ack(msg);
  } catch (e) {
    log.error(e);
    await queueBroker.nack(msg, true);
  }
}

export async function arweaveTxVerifier(msg) {
  const content = getContent(msg.content);
  const { txid } = content;
  try {
    const { status, confirmed } = await getStatus(txid);
    log.info(
      `txid: ${txid}: status: ${status}${
        confirmed ? `, details ${safeStringify(confirmed)}` : ''
      }`
    );
    if (status === 200 && confirmed?.number_of_confirmations > 10) {
      log.info('tx is valid so we can ack the msg and everything is ok');
      return await queueBroker.ack(msg);
    }
    log.info(
      `txid status is not yet confirmed with 10 transactions, waiting ${VERIFY_INTERVAL}s before nack message for txid: ${txid}`
    );
    await delay(VERIFY_INTERVAL);
    log.info(
      `For txid: ${txid} nack has been sent in order to recheck txid until it's confirmed`
    );
    return await queueBroker.nack(msg, true);
  } catch (e) {
    log.error({ e });
    await delay(VERIFY_INTERVAL);
    return queueBroker.nack(msg, true);
  }
}
