import { Consumer } from 'pulsar-client';

export type ServerOptions = {
  host: string;

  tenant: string;
  namespace: string;
  consumer: ConsumerOptions;
  webPort?: number;
  brokerPort?: number;
  tls?: boolean;

  create?: boolean;
  autoAck?: boolean;
};

export type ClientOptions = {
  host: string;
  tenant: string;
  namespace: string;
  producer: ProducerOptions;
  brokerPort?: number;
  tls?: boolean;
};

type ConsumerOptions = {
  subscriptionId: string;
};

type ProducerOptions = {
  sendTimeoutMs: number;
  batchingEnabled: boolean;
};

export type EventHandler = {
  data: any;
  consumer: Consumer;
};
