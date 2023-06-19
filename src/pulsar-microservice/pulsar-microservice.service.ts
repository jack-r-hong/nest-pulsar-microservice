import { HttpService } from '@nestjs/axios';
import { Injectable, Logger } from '@nestjs/common';
import { CustomTransportStrategy, Server } from '@nestjs/microservices';
import { AxiosError } from 'axios';
import * as Pulsar from 'pulsar-client';
import { Observable, catchError, firstValueFrom } from 'rxjs';
import { ServerOptions } from './interface';

const http = new HttpService();

@Injectable()
export class PulsarMicroserviceService
  extends Server
  implements CustomTransportStrategy
{
  transportId = Symbol('PULSAR');

  protected logger = new Logger(PulsarMicroserviceService.name);
  protected client: Pulsar.Client;
  private webUrl: string;
  private producers = new Map<string, Pulsar.Producer>();

  constructor(protected readonly options: ServerOptions) {
    super();

    if (!options.webPort) {
      options.webPort = 8080;
    }

    if (!options.brokerPort) {
      options.brokerPort = 6650;
    }
    this.webUrl = `${options.tls ? 'https' : 'http'}://${options.host}:${
      options.webPort
    }`;

    if (!options.autoAck) {
      this.options.autoAck = true;
    }
  }

  async listen(callback: () => void) {
    await this.createClient();

    await this.bindEvents();
    callback();
  }

  close() {
    this.client.close();
  }

  public async bindEvents() {
    const subscribePatterns = [...this.messageHandlers.keys()];

    await Promise.all(
      subscribePatterns.map(async (pattern) => {
        const { isEventHandler } = this.messageHandlers.get(pattern);
        const topic = `${isEventHandler ? 'event' : 'req'}-${pattern}`;
        return this.createConsumer(topic, async (message, consumer) => {
          const handler = this.messageHandlers.get(pattern);
          const data = JSON.parse(message.getData().toString());

          if (!isEventHandler) {
            const res = await handler(data.data, consumer);
            const producer = await this.createProducer(`res-${pattern}`);

            if (res instanceof Observable) {
              res.subscribe({
                next: (data) => {
                  console.log(data, 'data');
                },
                error: (e) => {
                  console.log({
                    err: e.message,
                    // status: 'error',
                    // message: e.message,
                    packetId: data.packetId,
                  });

                  producer.send({
                    data: Buffer.from(
                      JSON.stringify({
                        err: e.message,
                        // status: 'error',
                        // message: e.message,
                        packetId: data.packetId,
                      }),
                    ),
                    deliverAfter: 1000,
                  });
                },
                complete: () => {
                  console.log('complete');
                },
              });
            } else if (res) {
              producer.send({
                data: Buffer.from(
                  JSON.stringify({ data: res, packetId: data.packetId }),
                ),
                deliverAfter: 1000,
              });
            }
          } else {
            await handler(data, consumer);
          }

          if (this.options.autoAck) {
            await consumer.acknowledge(message).catch((e) => {
              console.log(e, 'ack error');
            });
          }
        });
      }),
    );
  }

  private async createClient() {
    const { host, brokerPort, create, namespace } = this.options;
    if (create) {
      await this.checkAndCreateTenant();
      await this.checkAndCreateNamespace(namespace);
      const namespaces = ['chat', 'channel-rec', 'channel-send-line'];
      for await (const namespace of namespaces) {
        await this.checkAndCreateNamespace(namespace);
      }
    }

    this.client = new Pulsar.Client({
      serviceUrl: `pulsar://${host}:${brokerPort}`,
    });

    this.logger.log('create client success');
  }

  private async createConsumer(
    topic: string,
    listener: (message: Pulsar.Message, consumer: Pulsar.Consumer) => void,
  ) {
    const { tenant, namespace } = this.options;

    await this.client.subscribe({
      topic: `persistent://${tenant}/${namespace}/${topic}`,
      subscriptionType: 'Exclusive',
      subscription: `sub-node-${process.env.NODE_NO ?? 1}`,
      listener,
    });
  }

  private async createProducer(topic: string) {
    const { tenant, namespace } = this.options;

    const producer = this.producers.get(topic);

    if (producer) {
      return producer;
    }

    const newProducer = await this.client.createProducer({
      topic: `persistent://${tenant}/${namespace}/${topic}`,
    });

    this.producers.set(topic, newProducer);

    return newProducer;
  }

  private async checkAndCreateTenant() {
    const { tenant } = this.options;
    const resTenant = await firstValueFrom(
      http.get(`${this.webUrl}/admin/v2/tenants/${tenant}`).pipe(
        catchError((e: AxiosError) => {
          throw e;
        }),
      ),
    ).catch((e) => {
      return null;
    });

    if (!resTenant) {
      const resCreateTenant = await firstValueFrom(
        http
          .put(`${this.webUrl}/admin/v2/tenants/${tenant}`, {
            adminRoles: [],
            allowedClusters: ['standalone'],
          })
          .pipe(
            catchError((e: AxiosError) => {
              console.log('create tenant error');
              throw e;
            }),
          ),
      );

      this.logger.log('create client');
    }
  }

  private async checkAndCreateNamespace(namespace: string) {
    const { tenant } = this.options;
    const resNameSpace = await firstValueFrom(
      http
        .get(`${this.webUrl}/admin/v2/namespaces/${tenant}/${namespace}`)
        .pipe(
          catchError((e: AxiosError) => {
            console.log(e);
            throw e;
          }),
        ),
    ).catch((e) => {
      return null;
    });

    if (!resNameSpace) {
      await firstValueFrom(
        http
          .put(`${this.webUrl}/admin/v2/namespaces/${tenant}/${namespace}`, {})
          .pipe(
            catchError((e: AxiosError) => {
              console.log(e);
              throw e;
            }),
          ),
      ).catch((e) => {
        return null;
      });
    }
  }
}
