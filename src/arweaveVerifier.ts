import { request as gqlRequest } from 'graphql-request';
import config from 'config';
import axios from 'axios';
import { ConsumeMessage } from 'amqplib';
import { getContent } from './utils/getContent';
import { queueBroker, QueueInfo } from './utils/queueBroker';
import getDownloadQuery from './utils/getDownloadQuery';
import { hashFn } from './utils/hashFn';
import { log } from './utils/logger';
import { safeStringify } from './utils/safeStringify';
import { getStatus } from './arweaveTransport';
import {
  getRandomTimeInMinutes,
  MILLISECONDS_IN_SECOND,
  SECONDS_IN_MINUTE,
} from './utils/getRandomTimeInMinutes';
import {
  UPLOAD_TO_ARWEAVE,
  VERIFY_ARWEAVE_TX,
  VERIFY_CHUNK_ID,
} from './utils/queueNames';

const SEND_IMMEDIATELY = 0;
const DISCARD_ARWEAVE_TX_TIMEOUT =
  config.get('arweave_tx_verifier.discard_tx_timeout') *
  MILLISECONDS_IN_SECOND *
  SECONDS_IN_MINUTE;
const TX_CONFIRMATIONS = config.get(
  'arweave_tx_verifier.min_txs_confirmations'
);
const GATEWAY_URL: string = config.get('storage.arweave_gateway_url');

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

const chunkIdVerifierCfg = {
  maxConcurrency: config.get('chunk_id_verifier.max_concurrency'),
};

export function chunkIdVerifierFactory({ channel }: QueueInfo) {
  return async function chunkIdVerifier(msg: ConsumeMessage | null) {
    const content = getContent(msg!.content);
    const { chunkId } = content;
    try {
      const arweaveTxId = await getTxIdForChunkId(chunkId);
      if (!arweaveTxId) {
        log.info(
          `Not found in arweave chunkId: ${chunkId}, sending message to arweaveUploader`
        );
        await queueBroker.sendMessage(UPLOAD_TO_ARWEAVE, { ...content });
      } else {
        log.info(`Found in arweave chunkId ${chunkId} txid: ${arweaveTxId}`);
      }
      channel.ack(msg!);
    } catch (error: any) {
      console.log({ error, keys: error });
      log.info(
        `Due to previous error we will wait before checking again chunkId: ${chunkId}`
      );
      await queueBroker.sendDelayedMessage(VERIFY_CHUNK_ID, content, {
        ...chunkIdVerifierCfg,
        ttl: getRandomTimeInMinutes(
          config.get('chunk_id_verifier.requeue_after_error_min'),
          config.get('chunk_id_verifier.requeue_after_error_max')
        ),
      });
      channel.ack(msg!);
    }
  };
}

const arweaveTxVerifierCfg = {
  maxConcurrency: config.get('arweave_tx_verifier.max_concurrency'),
};

export function arweaveTxVerifierFactory(queueInfo: QueueInfo) {
  const { channel } = queueInfo;
  return async function arweaveTxVerifier(msg: ConsumeMessage | null) {
    const content = getContent(msg!.content);
    const { txid, createdAt } = content;
    try {
      const { status, confirmed } = await getStatus(txid);
      log.info(
        `status ${status} for txid: ${txid} ${
          confirmed ? `, details ${safeStringify(confirmed)}` : ''
        }`
      );
      if (
        status === 200 &&
        confirmed?.number_of_confirmations > TX_CONFIRMATIONS
      ) {
        log.info(
          `Valid tx ${txid} with ${confirmed.number_of_confirmations} confirmations so we can ack the msg and everything is ok`
        );
        return channel.ack(msg!);
      }
      if (
        status === 200 &&
        confirmed?.number_of_confirmations < TX_CONFIRMATIONS
      ) {
        log.info(
          `txid status ${status} is not yet confirmed only ${confirmed?.number_of_confirmations} confirmations for txid: ${txid}, sending to delayed queue`
        );
        await queueBroker.sendDelayedMessage(VERIFY_ARWEAVE_TX, content, {
          ttl: getRandomTimeInMinutes(
            config.get('arweave_tx_verifier.min_verify_interval'),
            config.get('arweave_tx_verifier.max_verify_interval')
          ),
        });
        return channel.ack(msg!);
      }
      const age = Date.now() - createdAt;
      if (status === 404 && age > DISCARD_ARWEAVE_TX_TIMEOUT) {
        const { chunkId, fields } = content;
        log.fatal(
          `Timeout with status: ${status} for txid: ${txid} Reenqueue to check chunkId again and eventually upload to arweave again`
        );
        await queueBroker.sendDelayedMessage(
          VERIFY_CHUNK_ID,
          { chunkId, fields },
          { ttl: SEND_IMMEDIATELY }
        );
        channel.ack(msg!);
        return;
      }
      log.error(
        `tx is not in a valid status: ${status} for txid: ${txid}. Age ${Math.trunc(
          age / MILLISECONDS_IN_SECOND
        )}s. Because age is lower than ${Math.trunc(
          DISCARD_ARWEAVE_TX_TIMEOUT / MILLISECONDS_IN_SECOND
        )}s`
      );
      log.info(
        `Sent to delayed queue due to invalid status ${status} for txid: ${txid}  It will recheck txid until it has enough confirmations`
      );
      await queueBroker.sendDelayedMessage(VERIFY_ARWEAVE_TX, content, {
        ttl: getRandomTimeInMinutes(
          config.get('arweave_tx_verifier.min_verify_interval'),
          config.get('arweave_tx_verifier.max_verify_interval')
        ),
      });
      channel.ack(msg!);
      return;
    } catch (e) {
      console.log({ e });
      // log.error({ e });
      log.info(
        `Due to previous error we will wait before checking again txid: ${txid}`
      );
      queueBroker.pause(queueInfo, {
        healthCheckFunc: async () => {
          return true;
        },
        healthCheckInterval: 5,
      });
      await queueBroker.sendDelayedMessage(VERIFY_ARWEAVE_TX, content, {
        ...arweaveTxVerifierCfg,
        ttl: getRandomTimeInMinutes(
          config.get('arweave_tx_verifier.min_requeue_after_error_time'),
          config.get('arweave_tx_verifier.max_requeue_after_error_time')
        ),
      });
      channel.ack(msg!);
    }
  };
}
