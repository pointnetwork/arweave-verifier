import { request as gqlRequest } from 'graphql-request';
import config from 'config';
import axios from 'axios';
import { ConsumeMessage } from 'amqplib';
import { Bundle } from 'arbundles';
import base64url from 'base64url';
import crypto from 'crypto';
import { getContent } from './utils/getContent';
import { queueBroker, QueueInfo } from './utils/queueBroker';
import getDownloadQuery from './utils/getDownloadQuery';
import { hashFn } from './utils/hashFn';
import { log } from './utils/logger';
import { safeStringify } from './utils/safeStringify';
import { getStatus } from './arweaveTransport';
import {
  REUPLOAD_TO_ARWEAVE,
  UPLOAD_TO_ARWEAVE,
  VERIFY_BUNDLED_TX,
  VERIFY_CHUNK_ID,
  VERIFY_CHUNK_ID_LONG,
} from './utils/queueNames';
import {
  fromMinutesToMilliseconds,
  MILLISECONDS_IN_SECOND,
} from './utils/fromMinutesToMilliseconds';

const HEALTH_CHECK_CHUNK_ID =
  '09cbb8571a50d8eb44933bb261ed8280d9528aee408fb2f90dba15ab6f952f94';
const DISCARD_ARWEAVE_TX_TIMEOUT = fromMinutesToMilliseconds(
  config.get('arweave_tx_verifier.discard_tx_timeout')
);
const TX_CONFIRMATIONS = config.get(
  'arweave_tx_verifier.min_txs_confirmations'
);
const GATEWAY_URL: string = config.get('storage.arweave_gateway_url');
const BUNDLED_CHUNK_ID_TIMEOUT = config.get(
  'chunk_id_verifier.bundled_chunk_id_timeout'
);

export async function downloadFileUrl(fileUrl: string) {
  const fileStream = await axios.get(fileUrl, {
    responseType: 'stream',
    timeout: 120000,
  });
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

export function chunkIdVerifierFactory(queueInfo: QueueInfo) {
  const { channel } = queueInfo;
  return async function chunkIdVerifier(msg: ConsumeMessage | null) {
    const content = getContent(msg!.content);
    const { chunkId, createdAt } = content;
    try {
      const arweaveTxId = await getTxIdForChunkId(chunkId);
      const chunkIdNotFound = !arweaveTxId;
      const chunkIdWasUploaded = !!createdAt;
      if (chunkIdNotFound) {
        if (chunkIdWasUploaded) {
          if (
            Date.now() - createdAt >
            fromMinutesToMilliseconds(BUNDLED_CHUNK_ID_TIMEOUT)
          ) {
            log.info(
              `Chunk in bundle was not found after a long time for chunkId: ${chunkId}. Sending message to arweaveUploader`
            );
            await queueBroker.sendMessage(UPLOAD_TO_ARWEAVE, { ...content });
          } else {
            log.info(
              `Chunk in bundle was not found for chunkId: ${chunkId} Will try again until timeout is reached`
            );
            await queueBroker.sendDelayedMessage(
              VERIFY_CHUNK_ID_LONG,
              content,
              {
                ttl: fromMinutesToMilliseconds(
                  config.get('arweave_tx_verifier.requeue_after_error_time')
                ),
              }
            );
          }
        } else {
          log.info(
            `Not found in arweave chunkId: ${chunkId}, sending message to arweaveUploader`
          );
          await queueBroker.sendMessage(UPLOAD_TO_ARWEAVE, { ...content });
        }
      } else {
        log.info(`Found in arweave chunkId ${chunkId} txid: ${arweaveTxId}`);
      }
      channel.ack(msg!);
    } catch (error: any) {
      log.error(
        `Due to error will postpone arweave checking for chunkId: ${chunkId} Error Details: ${safeStringify(
          error
        )}`
      );
      await queueBroker.sendDelayedMessage(VERIFY_CHUNK_ID, content, {
        ttl: fromMinutesToMilliseconds(
          config.get('arweave_tx_verifier.requeue_after_error_time')
        ),
      });
      channel.ack(msg!);
      if (error?.response?.status === 429) {
        queueBroker.pause(queueInfo, {
          healthCheckFunc: async () => {
            try {
              await getTxIdForChunkId(HEALTH_CHECK_CHUNK_ID);
              log.info(
                `Healthcheck has succeed for queue ${queueInfo.subscription?.name}. Will resume worker.`
              );
              queueBroker.resume(queueInfo);
              return true;
            } catch (healthCheckError: any) {
              log.error(
                `Healthcheck failed due to error: ${safeStringify(
                  healthCheckError
                )}`
              );
              return false;
            }
          },
          healthCheckInterval: config.get(
            'chunk_id_verifier.health_check_interval'
          ),
        });
      }
    }
  };
}

export function arweaveTxVerifierFactory(queueInfo: QueueInfo) {
  const { channel } = queueInfo;
  return async function arweaveTxVerifier(msg: ConsumeMessage | null) {
    const content = getContent(msg!.content);
    const { txid, createdAt, signatures } = content;
    try {
      const { status, confirmed } = await getStatus(txid);
      log.info(
        `status ${status} for txid: ${txid} ${
          confirmed ? `, details ${safeStringify(confirmed)}` : ''
        }`
      );
      if (status === 429) {
        await queueBroker.sendDelayedMessage(VERIFY_BUNDLED_TX, content, {
          ttl: fromMinutesToMilliseconds(
            config.get('arweave_tx_verifier.verify_interval')
          ),
        });
        channel.ack(msg!);
        queueBroker.pause(queueInfo, {
          healthCheckFunc: async () => {
            try {
              // eslint-disable-next-line @typescript-eslint/no-shadow
              const { status } = await getStatus(txid);
              if (status === 429) {
                log.error(
                  'Healthcheck failed for tx verifier due to status 429'
                );
                return false;
              }
              log.info(
                `Healthcheck has succeed for queue ${queueInfo.subscription?.name}. Will resume worker.`
              );
              queueBroker.resume(queueInfo);
              return true;
            } catch (healthCheckError: any) {
              log.error(
                `Healthcheck failed for tx verifier due to error: ${safeStringify(
                  healthCheckError
                )}`
              );
              return false;
            }
          },
          healthCheckInterval: config.get(
            'chunk_id_verifier.health_check_interval'
          ),
        });
        return;
      }
      if (
        [200, 202].includes(status) &&
        confirmed?.number_of_confirmations > TX_CONFIRMATIONS
      ) {
        log.info(
          `Valid tx ${txid} with ${confirmed.number_of_confirmations} confirmations. Will check bundle integrity`
        );
        try {
          const file = (await downloadFileUrl(
            `https://arweave.net/${txid}`
          )) as Buffer;
          const bundle = new Bundle(file);
          if (await bundle.verify()) {
            const ids = bundle.getIds();
            const calculatedIds = signatures
              .map((signature) => {
                return base64url.encode(
                  crypto
                    .createHash('sha256')
                    .update(base64url.toBuffer(signature))
                    .digest()
                );
              })
              .sort();
            const bundleIsOk = ids
              .sort()
              .every((id, ix) => id === calculatedIds[ix]);
            if (bundleIsOk) {
              log.info(`Bundle integrity is ok for valid tx ${txid}`);
              return channel.ack(msg!);
            }
          }
          throw Error('Bundle verification failed');
        } catch (error: any) {
          console.log({ error });
          if (error.response?.status === 404) {
            // the tx is ok but the content is not there, we should reupload the content of the tx
            await queueBroker.sendDelayedMessage(REUPLOAD_TO_ARWEAVE, content, {
              ttl: 1000 * 60,
            });
          }
        }
        return channel.ack(msg!);
      }
      if (
        [200, 202].includes(status) &&
        confirmed?.number_of_confirmations < TX_CONFIRMATIONS
      ) {
        log.info(
          `txid status ${status} is not yet confirmed only ${confirmed?.number_of_confirmations} confirmations for txid: ${txid}, sending to delayed queue`
        );
        await queueBroker.sendDelayedMessage(VERIFY_BUNDLED_TX, content, {
          ttl: fromMinutesToMilliseconds(
            config.get('arweave_tx_verifier.verify_interval')
          ),
        });
        return channel.ack(msg!);
      }
      const age = Date.now() - createdAt;
      if (status === 404 && age > DISCARD_ARWEAVE_TX_TIMEOUT) {
        const { chunkIds } = content;
        log.fatal(
          `Timeout with status: ${status} for bundled txid: ${txid} Reenqueue to check chunkId again and eventually upload to arweave again`
        );
        await queueBroker.sendMessage('failedTxs', { chunkIds, txid });
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
      await queueBroker.sendDelayedMessage(VERIFY_BUNDLED_TX, content, {
        ttl: fromMinutesToMilliseconds(
          config.get('arweave_tx_verifier.verify_interval')
        ),
      });
      channel.ack(msg!);
      return;
    } catch (error: any) {
      log.error(
        `Due to error will postpone arweave checking for txid: ${txid} Error Details: ${safeStringify(
          error
        )}`
      );
      await queueBroker.sendDelayedMessage(VERIFY_BUNDLED_TX, content, {
        ttl: fromMinutesToMilliseconds(
          config.get('arweave_tx_verifier.requeue_after_error_time')
        ),
      });
      channel.ack(msg!);
    }
  };
}
