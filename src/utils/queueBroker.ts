import {
  Connection,
  Channel,
  ConsumeMessage,
  connect,
  Options,
  Replies,
} from 'amqplib';
import config from 'config';
import { log } from './logger';
import { safeStringify } from './safeStringify';

const WORK_PREFIX = 'work';
const LOBBY_PREFIX = 'lobby';

const queueCfg: { url: string } | undefined = config.get('queue');
interface QueueBrokerOptions {
  url?: string;
}

export interface QueueInfo {
  channel: Channel;
  queue: Replies.AssertQueue;
}

export interface QueueCfg {
  maxConcurrency?: number;
}

export interface DelayedQueueCfg extends QueueCfg {
  ttl: number;
}

const defaultOptions = { url: 'amqp://localhost' };

let resolver;

export class QueueBroker {
  connection?: Connection;

  channelsByQueueName: Record<string, QueueInfo> = Object.create(null);

  ready = new Promise((res) => {
    resolver = res;
  });

  constructor(private options: QueueBrokerOptions = defaultOptions) {}

  async connect() {
    const queueUrl = this.options.url as string;
    this.connection = await connect(queueUrl);
    log.info('Connected to queue');
    resolver();
  }

  async ensureChannelAndQueue(
    name: string,
    options: QueueCfg = {}
  ): Promise<QueueInfo> {
    await this.ready;
    if (!this.channelsByQueueName[name]) {
      const channel = await this.connection!.createChannel();
      const queue = await channel.assertQueue(name);
      if (options.maxConcurrency) {
        channel.prefetch(options.maxConcurrency);
      }
      this.channelsByQueueName[name] = { channel, queue };
    }
    return this.channelsByQueueName[name];
  }

  async ensureChannelAndDelayedQueue(name: string, options: QueueCfg = {}) {
    const nameWithPrefix = `${WORK_PREFIX}--${name}`;
    await this.ready;
    if (!this.channelsByQueueName[nameWithPrefix]) {
      const channel = await this.connection!.createChannel();
      if (options.maxConcurrency) {
        channel.prefetch(options.maxConcurrency);
      }
      const exchangeDLX = `${name}ExDLX`;
      const routingKeyDLX = `${name}RoutingKeyDLX`;
      const queueDLX = `${name}Work`;
      await channel.assertExchange(exchangeDLX, 'direct', {
        durable: true,
      });
      const queue = (await channel.assertQueue(queueDLX, {
        exclusive: false,
      })) as Replies.AssertQueue;
      await channel.bindQueue(queue.queue, exchangeDLX, routingKeyDLX);
      this.channelsByQueueName[nameWithPrefix] = { channel, queue };
    }
    return this.channelsByQueueName[nameWithPrefix];
  }

  async subscribe(
    queueName: string,
    handlerFactory: (
      queueInfo: QueueInfo
    ) => (msg: ConsumeMessage | null) => void
  ) {
    const queueInfo = await this.ensureChannelAndQueue(queueName);
    const { channel, queue } = queueInfo;
    log.info(`Subscribing to queue ${queue.queue}`);
    channel.consume(queue.queue, handlerFactory(queueInfo));
  }

  async subscribeDelayed(
    queueName: string,
    handlerFactory: (
      queueInfo: QueueInfo
    ) => (msg: ConsumeMessage | null) => void,
    options: QueueCfg = {}
  ) {
    const queueInfo = await this.ensureChannelAndDelayedQueue(
      queueName,
      options
    );
    const { channel, queue } = queueInfo;
    log.info(`Subscribing to delayed queue ${queue.queue}`);
    await channel.consume(queue.queue, handlerFactory(queueInfo));
  }

  async ensureChannelAndLobby(
    name: string,
    options: DelayedQueueCfg = { ttl: 0 }
  ) {
    const nameWithPrefix = `${LOBBY_PREFIX}--${name}`;
    await this.ready;
    if (!this.channelsByQueueName[nameWithPrefix]) {
      const channel = await this.connection!.createChannel();
      if (options.maxConcurrency) {
        channel.prefetch(options.maxConcurrency);
      }
      const exchange = `${name}Exchange`;
      const queueName = `${name}Lobby`;
      const exchangeDLX = `${name}ExDLX`;
      const routingKeyDLX = `${name}RoutingKeyDLX`;
      await channel.assertExchange(exchange, 'direct', {
        durable: true,
      });
      const queue = (await channel?.assertQueue(queueName, {
        exclusive: false,
        deadLetterExchange: exchangeDLX,
        deadLetterRoutingKey: routingKeyDLX,
      })) as Replies.AssertQueue;
      await channel?.bindQueue(queue.queue, exchange, '');
      this.channelsByQueueName[nameWithPrefix] = { channel, queue };
    }
    return this.channelsByQueueName[nameWithPrefix];
  }

  async sendDelayedMessage(
    queueName: string,
    content: Record<any, any>,
    options: DelayedQueueCfg
  ) {
    const { channel, queue } = await this.ensureChannelAndLobby(
      queueName,
      options
    );
    return channel.sendToQueue(
      queue.queue,
      Buffer.from(JSON.stringify(content)),
      {
        expiration: options.ttl,
      }
    );
  }

  async sendMessage(
    queueName: string,
    content: Record<any, any>,
    options?: Options.Publish
  ) {
    const { channel, queue } = await this.ensureChannelAndQueue(queueName);
    log.info(
      `Sending message to ${queueName} with content ${safeStringify(content)}`
    );
    return channel.sendToQueue(
      queue.queue,
      Buffer.from(JSON.stringify(content)),
      options
    );
  }
}

export const queueBroker = queueCfg
  ? new QueueBroker(queueCfg)
  : new QueueBroker();

async function connectQueue() {
  try {
    await queueBroker.connect();
  } catch (error: any) {
    log.error('Cannot connect to queue, check connectivity. Exiting process');
    process.exit(1);
  }
}

connectQueue();
