import { Module } from '@nestjs/common';
import { PulsarMicroserviceService } from './pulsar-microservice.service';
import { PulsarClientService } from './pulsar-client.service';

@Module({
  providers: [PulsarMicroserviceService, PulsarClientService],
  exports: [PulsarMicroserviceService, PulsarClientService],
})
export class PulsarMicroserviceModule {}
