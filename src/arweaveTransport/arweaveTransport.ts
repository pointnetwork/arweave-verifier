import { arweave as arweaveInstance } from './arweave';

export const DEFAULT_RETRY_POLICY = {
  retries: 5,
  minTimeout: 3000,
};

class ArweaveTransport {
  private arweave = arweaveInstance;

  public async getStatus(txid) {
    return this.arweave.transactions.getStatus(txid);
  }
}

export const arweaveTransport = new ArweaveTransport();
