import { Inject, Injectable, Logger } from '@nestjs/common';
import { ClientProxy, ReadPacket, WritePacket } from '@nestjs/microservices';
import { ClientOptions } from './interface';
import Pulsar from 'pulsar-client';

export const createClient = (host: string, port: string) => {
  return new Pulsar.Client({
    serviceUrl: `pulsar://${host}:${port}`,
  });
};

@Injectable()
export class PulsarClientService extends ClientProxy {
  protected logger = new Logger(PulsarClientService.name);
  protected client: Pulsar.Client;
  private static instance: PulsarClientService;
  private static hostAndPortMap: Map<string, boolean> = new Map();
  protected producters: Map<string, Pulsar.Producer> = new Map();
  protected consumers: Map<string, Pulsar.Consumer> = new Map();
  protected routingMap: Map<string, (packet: WritePacket<any>) => void> =
    new Map();

  private nodeName = 'node';

  constructor(protected readonly options: ClientOptions) {
    super();

    if (!this.options.brokerPort) {
      this.options.brokerPort = 6650;
    }

    const hostAndPort = `${this.options.host}:${this.options.brokerPort}`;
    if (PulsarClientService.hostAndPortMap.get(hostAndPort)) {
      return PulsarClientService.instance;
    }

    PulsarClientService.hostAndPortMap.set(hostAndPort, true);
  }

  async connect(): Promise<any> {
    // this.logger.log('connect');
    const { host, brokerPort } = this.options;
    if (!this.client) {
      this.client = new Pulsar.Client({
        serviceUrl: `pulsar://${host}:${brokerPort}`,
      });

      this.logger.log('PulsarClientService create');
    }
  }
  async close() {
    this.client.close();
    this.logger.log('client close');
  }
  async dispatchEvent(packet: ReadPacket<any>): Promise<any> {
    const producer = await this.getProducer(packet, true);

    return producer
      .send({
        data: Buffer.from(JSON.stringify(packet.data)),
        deliverAfter: 1000,
      })
      .catch((err) => {
        this.logger.error(err);
      });
  }

  publish(
    partialPacket: ReadPacket<any>,
    callback: (packet: WritePacket<any>) => void,
  ) {
    const packet = this.assignPacketId(partialPacket);
    const cleanup = () => this.routingMap.delete(packet.id);
    const errorCallback = (err: unknown) => {
      cleanup();
      callback({ err });
    };

    // res topic
    const packetId = `${this.nodeName}-${packet.id}`;

    this.getConsumer(packet)
      .then(async () => {
        const producer = await this.getProducer(packet);
        // req topic
        await producer.send({
          data: Buffer.from(
            JSON.stringify({
              data: packet.data,
              packetId: packetId,
            }),
          ),
          deliverAfter: 1000,
        });

        this.routingMap.set(packetId, callback);
      })
      .catch(errorCallback);

    return () => this.cleanup(packetId);
  }

  protected async getProducer(packet: ReadPacket<any>, isEvent = false) {
    const { tenant, namespace, producer: ProducerOptions } = this.options;
    const topic = `${isEvent ? 'event' : 'req'}-${packet.pattern}`;
    let producer = this.producters.get(topic);

    if (!producer) {
      producer = await this.client.createProducer({
        topic: `persistent://${tenant}/${namespace}/${topic}`,
        sendTimeoutMs: ProducerOptions.sendTimeoutMs,
        batchingEnabled: ProducerOptions.batchingEnabled,
      });

      this.producters.set(topic, producer);
    }

    return producer;
  }

  protected async getConsumer(
    packet: ReturnType<ClientProxy['assignPacketId']>,
  ) {
    const { tenant, namespace } = this.options;
    const topic = `res-${packet.pattern}`;
    const key = `${tenant}/${namespace}/${topic}`;
    let consumer = this.consumers.get(key);
    const routingMap = this.routingMap;

    if (!consumer) {
      consumer = await this.client
        .subscribe({
          topic: `persistent://${tenant}/${namespace}/${topic}`,
          subscription: 'sub',
          subscriptionType: 'Shared',
          listener(message, consumer) {
            const response = JSON.parse(message.getData().toString());

            const callback = routingMap.get(response.packetId);

            if (!callback) {
              return;
            }

            if (response.err) {
              callback({
                err: response.err,
                response: null,
                isDisposed: true,
              });
            } else {
              callback({
                err: null,
                response: response.data,
                isDisposed: true,
              });
            }

            consumer.acknowledge(message);
          },
        })
        .catch((err) => {
          console.log('getConsumer error', err);

          throw err;
        });

      this.consumers.set(key, consumer);
    }

    return consumer;
  }

  protected cleanup(id: string) {
    this.routingMap.delete(id);
  }
}
