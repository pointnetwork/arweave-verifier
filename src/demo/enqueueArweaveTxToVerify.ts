// npx ts-node ./src/demo/enqueueArweaveTxToVerify.ts
import { queueBroker } from '../utils/queueBroker';

const message = {
  txid: 'GYfrcwEJuiROw2CznHctW4aAClnJJMuCAzsL4JOCUzs',
};

(async function () {
  await queueBroker.sendMessage('verifyArweaveTx', message);
})();
