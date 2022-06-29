import Fastify from 'fastify';
import { port } from 'config';
import { log } from './utils/logger';

const fastify = Fastify();

fastify.get('/health', function (request, reply) {
  reply.send({ health: 'ok' });
});

// Run the server!
fastify.listen({ port, host: '0.0.0.0' }, function (err, address) {
  if (err) {
    fastify.log.error(err);
    process.exit(1);
  }
  log.info(`Health Server is now listening on ${address}`);
});
