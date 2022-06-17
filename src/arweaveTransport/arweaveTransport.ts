import { arweave } from './arweave';

export async function getStatus(txid) {
  return arweave.transactions.getStatus(txid);
}
