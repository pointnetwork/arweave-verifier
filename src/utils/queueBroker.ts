import {
  Connection,
  Channel,
  ConsumeMessage,
  connect,
  Options,
  Replies,
} from 'amqplib';
import config from 'config';
import {
  MILLISECONDS_IN_SECOND,
  SECONDS_IN_MINUTE,
} from './getRandomTimeInMinutes';
import { log } from './logger';
import { safeStringify } from './safeStringify';

const WORK_PREFIX = 'work';
const LOBBY_PREFIX = 'lobby';

const queueCfg: { url: string } | undefined = config.get('queue');
interface QueueBrokerOptions {
  url?: string;
}

export interface QueueInfo {
  consumerTag?: string;
  channel: Channel;
  queue: Replies.AssertQueue;
  queueId: string;
  subscription?: {
    name: string;
    options: ConsumerQueueCfg;
    isPaused: boolean;
    isDelayed: boolean;
  };
}

export type HandlerFactory = (
  queueInfo: QueueInfo
) => (msg: ConsumeMessage | null) => void;

export interface ConsumerQueueCfg {
  handlerFactory: HandlerFactory;
  maxConcurrency?: number;
}

export interface DelayedQueueCfg {
  ttl: number;
}

const defaultOptions = { url: 'amqp://localhost' };

let resolver;

function scheduleCheck(healthCheckFunc, interval) {
  const timeoutId = setTimeout(async () => {
    if (await healthCheckFunc!()) {
      clearTimeout(timeoutId);
    } else {
      scheduleCheck(healthCheckFunc, interval);
    }
  }, interval);
}

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

  async pause(
    queueInfo: QueueInfo,
    {
      healthCheckFunc,
      healthCheckInterval,
    }: {
      healthCheckFunc?: () => Promise<boolean>;
      healthCheckInterval?: number;
    } = {
      healthCheckInterval: 5,
    }
  ) {
    const { channel, consumerTag, queueId, subscription } = queueInfo;
    const isPaused = subscription?.isPaused;
    if (!isPaused && consumerTag) {
      this.channelsByQueueName[queueId].subscription!.isPaused = true;
      await channel.cancel(consumerTag);
      scheduleCheck(
        healthCheckFunc,
        healthCheckInterval! * MILLISECONDS_IN_SECOND * SECONDS_IN_MINUTE
      );
      console.log('#$#$#$#$$$$$$%$%^^^^^^^^%%$');
      console.log('#$#$#$#$$$$$$%$%^^^^^^^^%%$');
      console.log('#$#$#$#$$$$$$%$%^^^^^^^^%%$');
      console.log('#$#$#$#$$$$$$%$%^^^^^^^^%%$');
      console.log('pause successfully');
      return true;
    }
    console.log('$$$$$$$$$$$$$$$$$$$$$$$$$$$');
    console.log('$$$$$$$$$$$$$$$$$$$$$$$$$$$');
    console.log('$$$$$$$$$$$$$$$$$$$$$$$$$$$');
    console.log('$$$$$$$$$$$$$$$$$$$$$$$$$$$');
    console.log('cound not pause');
    return false;
  }

  async resume(queueInfo: QueueInfo) {
    const { subscription } = queueInfo;
    const isPaused = subscription?.isPaused;
    if (
      !subscription ||
      (subscription && !isPaused) ||
      !(typeof subscription.options.handlerFactory === 'function')
    ) {
      return false;
    }
    const { name, options } = subscription;
    await (subscription.isDelayed
      ? this.subscribeDelayed(name, options)
      : this.subscribe(name, options));
    return true;
  }

  async ensureChannelAndQueue(
    name: string,
    options?: ConsumerQueueCfg
  ): Promise<QueueInfo> {
    await this.ready;
    if (!this.channelsByQueueName[name]) {
      const channel = await this.connection!.createChannel();
      const queue = await channel.assertQueue(name);
      if (options?.maxConcurrency) {
        channel.prefetch(options.maxConcurrency);
      }
      this.channelsByQueueName[name] = {
        channel,
        queue,
        queueId: name,
      };
    }
    return this.channelsByQueueName[name];
  }

  async ensureChannelAndDelayedQueue(name: string, options?: ConsumerQueueCfg) {
    const nameWithPrefix = `${WORK_PREFIX}--${name}`;
    await this.ready;
    if (!this.channelsByQueueName[nameWithPrefix]) {
      const channel = await this.connection!.createChannel();
      if (options?.maxConcurrency) {
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
      this.channelsByQueueName[nameWithPrefix] = {
        channel,
        queue,
        queueId: nameWithPrefix,
      };
    }
    return this.channelsByQueueName[nameWithPrefix];
  }

  async subscribe(queueName: string, options: ConsumerQueueCfg) {
    const { handlerFactory } = options;
    const queueInfo = await this.ensureChannelAndQueue(queueName, options);
    const { channel, queue, queueId } = queueInfo;
    log.info(`Subscribing to queue ${queue.queue}`);
    const { consumerTag } = await channel.consume(
      queue.queue,
      handlerFactory(queueInfo)
    );
    this.channelsByQueueName[queueId].consumerTag = consumerTag;
    this.channelsByQueueName[queueId].subscription = {
      name: queueName,
      options,
      isPaused: false,
      isDelayed: false,
    };
  }

  async subscribeDelayed(queueName: string, options: ConsumerQueueCfg) {
    const queueInfo = await this.ensureChannelAndDelayedQueue(
      queueName,
      options
    );
    const { channel, queue, queueId } = queueInfo;
    const { handlerFactory } = options;
    log.info(`Subscribing to delayed queue ${queue.queue}`);
    const { consumerTag } = await channel.consume(
      queue.queue,
      handlerFactory(queueInfo)
    );
    this.channelsByQueueName[queueId].consumerTag = consumerTag;
    this.channelsByQueueName[queueId].subscription = {
      name: queueName,
      options,
      isPaused: false,
      isDelayed: true,
    };
  }

  async ensureChannelAndLobby(name: string, options?: ConsumerQueueCfg) {
    const nameWithPrefix = `${LOBBY_PREFIX}--${name}`;
    await this.ready;
    if (!this.channelsByQueueName[nameWithPrefix]) {
      const channel = await this.connection!.createChannel();
      if (options?.maxConcurrency) {
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
      this.channelsByQueueName[nameWithPrefix] = {
        channel,
        queue,
        queueId: nameWithPrefix,
      };
    }
    return this.channelsByQueueName[nameWithPrefix];
  }

  async sendDelayedMessage(
    queueName: string,
    content: Record<any, any>,
    options: DelayedQueueCfg
  ) {
    const { channel, queue } = await this.ensureChannelAndLobby(queueName);
    const queueId = options.ttl === 0 ? `${queueName}Work` : queue.queue;
    return channel.sendToQueue(queueId, Buffer.from(JSON.stringify(content)), {
      ...(options.ttl > 0 ? { expiration: options.ttl } : {}),
    });
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
