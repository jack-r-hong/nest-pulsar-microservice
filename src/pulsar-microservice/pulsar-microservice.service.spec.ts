import { Test, TestingModule } from '@nestjs/testing';
import { PulsarMicroserviceService } from './pulsar-microservice.service';

describe('PulsarMicroserviceService', () => {
  let service: PulsarMicroserviceService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [PulsarMicroserviceService],
    }).compile();

    service = module.get<PulsarMicroserviceService>(PulsarMicroserviceService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
