import { Test, TestingModule } from '@nestjs/testing';

import { AddressSettingsService } from '../../ORM/address-settings/address-settings.service';
import { ClientStatisticsService } from '../../ORM/client-statistics/client-statistics.service';
import { ClientService } from '../../ORM/client/client.service';
import { PayoutSnapshotService } from '../../ORM/payout-snapshot/payout-snapshot.service';
import { ShareAccountingService } from '../../ORM/share-accounting/share-accounting.service';
import { ClientController } from './client.controller';

describe('ClientController', () => {
  let controller: ClientController;
  let clientService: { getByAddress: jest.Mock; getBySessionId: jest.Mock };
  let addressSettingsService: { getSettings: jest.Mock };
  let shareAccountingService: {
    getAddressSummary: jest.Mock;
    getWorkerGroupSummary: jest.Mock;
    getSessionSummary: jest.Mock;
    getSessionSummaries: jest.Mock;
  };
  let payoutSnapshotService: { getLatestExpectedPayoutForAddress: jest.Mock };

    beforeEach(async () => {
        const module: TestingModule = await Test.createTestingModule({
            controllers: [ClientController],
            providers: [
                {
                    provide: ClientService,
                    useValue: {
                        getByAddress: jest.fn().mockResolvedValue([]),
                        getByName: jest.fn(),
                        getBySessionId: jest.fn(),
                    },
                },
                {
                    provide: ClientStatisticsService,
                    useValue: {
                        getChartDataForAddress: jest.fn(),
                        getChartDataForGroup: jest.fn(),
                        getChartDataForSession: jest.fn(),
                    },
                },
                {
                    provide: AddressSettingsService,
                    useValue: {
                        getSettings: jest.fn(),
                    },
                },
                {
                    provide: ShareAccountingService,
                    useValue: {
                        getAddressSummary: jest.fn(),
                        getWorkerGroupSummary: jest.fn(),
                        getSessionSummary: jest.fn(),
                        getSessionSummaries: jest.fn().mockResolvedValue(new Map()),
                    },
                },
                {
                    provide: PayoutSnapshotService,
                    useValue: {
                        getLatestExpectedPayoutForAddress: jest.fn().mockResolvedValue(null),
                    },
                },
            ],

        }).compile();

    controller = module.get<ClientController>(ClientController);
    clientService = module.get(ClientService);
    addressSettingsService = module.get(AddressSettingsService);
    shareAccountingService = module.get(ShareAccountingService);
    payoutSnapshotService = module.get(PayoutSnapshotService);
  });

  it('should be defined', () => {
    expect(controller).toBeDefined();
  });

  it('should expose the existing address best difficulty in accounting when rollup best is empty', async () => {
    clientService.getByAddress.mockResolvedValue([
      {
        id: '92f5302f-5e32-487e-af67-f56fd78b13c7',
        sessionId: 'abcd1234',
        clientName: 'worker',
        bestDifficulty: 64,
        hashRate: 1024,
        startTime: '2026-06-08T12:00:00.000Z',
        lastSeen: '2026-06-08T12:10:00.000Z',
        address: 'bc1qtest',
        payoutMode: 'pplns',
      },
    ]);
    addressSettingsService.getSettings.mockResolvedValue({ bestDifficulty: 4096 });
    shareAccountingService.getSessionSummaries.mockResolvedValue(new Map());
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 10,
      totalCreditedDifficulty: 100,
      bestSubmissionDifficulty: 0,
    });

    await expect(controller.getClientInfo('bc1qtest')).resolves.toEqual(expect.objectContaining({
      bestDifficulty: 4096,
      accounting: expect.objectContaining({
        bestSubmissionDifficulty: 4096,
      }),
    }));
  });

  it('should read address best difficulty in API-only mode', async () => {
    const originalApiOnly = process.env.API_ONLY;
    process.env.API_ONLY = 'true';
    try {
      addressSettingsService.getSettings.mockResolvedValue({ bestDifficulty: 8192 });
      shareAccountingService.getSessionSummaries.mockResolvedValue(new Map());
      shareAccountingService.getAddressSummary.mockResolvedValue({
        totalAcceptedShares: 10,
        totalCreditedDifficulty: 100,
        bestSubmissionDifficulty: 0,
      });

      await expect(controller.getClientInfo('bc1qtest')).resolves.toEqual(expect.objectContaining({
        bestDifficulty: 8192,
        accounting: expect.objectContaining({
          bestSubmissionDifficulty: 8192,
        }),
      }));
      expect(addressSettingsService.getSettings).toHaveBeenCalledWith('bc1qtest', false);
    } finally {
      if (originalApiOnly == null) {
        delete process.env.API_ONLY;
      } else {
        process.env.API_ONLY = originalApiOnly;
      }
    }
  });

  it('should zero a stale session hashRate instead of serving the frozen estimate', async () => {
    const now = Date.now();
    clientService.getByAddress.mockResolvedValue([
      {
        id: 'f1e2d3c4-0000-4000-8000-000000000001',
        sessionId: '6fab2a1f',
        clientName: 'o46',
        bestDifficulty: 100,
        // Entity hashRate is written share-event-side: this is the last
        // in-burst estimate, frozen since the burst ended.
        hashRate: 2_800_000_000_000,
        startTime: new Date(now - 3 * 60 * 60 * 1000).toISOString(),
        // updatedAt stays fresh without shares (keeps the worker listed) —
        // it is NOT a share-recency signal.
        updatedAt: new Date(now - 2 * 60 * 1000).toISOString(),
        address: 'bc1qtest',
        payoutMode: 'solo',
      },
    ]);
    addressSettingsService.getSettings.mockResolvedValue(null);
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 10,
      totalCreditedDifficulty: 100,
      bestSubmissionDifficulty: 0,
    });
    const lastShareAt = new Date(now - 56 * 60 * 1000).toISOString();
    shareAccountingService.getSessionSummaries.mockResolvedValue(new Map([
      ['f1e2d3c4-0000-4000-8000-000000000001', {
        bestSubmissionDifficulty: 16_500_000,
        latestShareAt: lastShareAt,
        hashRateLast10Minutes: 0,
      }],
    ]));

    const response = await controller.getClientInfo('bc1qtest');
    expect(response.workers).toHaveLength(1);
    expect(response.workers[0].hashRate).toBe(0);
    expect(response.workers[0].lastSeen).toBe(lastShareAt);
  });

  it('should keep serving the entity hashRate while the session is share-fresh', async () => {
    const now = Date.now();
    clientService.getByAddress.mockResolvedValue([
      {
        id: 'f1e2d3c4-0000-4000-8000-000000000002',
        sessionId: 'abcd1234',
        clientName: 'o80',
        bestDifficulty: 100,
        hashRate: 1_200_000_000_000,
        startTime: new Date(now - 60 * 60 * 1000).toISOString(),
        updatedAt: new Date(now - 60 * 1000).toISOString(),
        address: 'bc1qtest',
        payoutMode: 'solo',
      },
    ]);
    addressSettingsService.getSettings.mockResolvedValue(null);
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 10,
      totalCreditedDifficulty: 100,
      bestSubmissionDifficulty: 0,
    });
    shareAccountingService.getSessionSummaries.mockResolvedValue(new Map([
      ['f1e2d3c4-0000-4000-8000-000000000002', {
        bestSubmissionDifficulty: 500,
        latestShareAt: new Date(now - 2 * 60 * 1000).toISOString(),
        hashRateLast10Minutes: 0,
      }],
    ]));

    const response = await controller.getClientInfo('bc1qtest');
    expect(response.workers).toHaveLength(1);
    expect(response.workers[0].hashRate).toBe(1_200_000_000_000);
  });

  it('should expose the latest current PPLNS expected payout for an address', async () => {
    addressSettingsService.getSettings.mockResolvedValue(null);
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 10,
      totalCreditedDifficulty: 100,
      bestSubmissionDifficulty: 0,
    });
    payoutSnapshotService.getLatestExpectedPayoutForAddress.mockResolvedValue({
      snapshotId: '19',
      blockHeight: 900001,
      payoutMode: 'pplns',
      payoutSats: 1234,
      creditedDifficulty: 50,
      percent: 12.34,
    });

    await expect(controller.getClientInfo('bc1qtest')).resolves.toEqual(expect.objectContaining({
      expectedPayout: expect.objectContaining({
        snapshotId: '19',
        payoutSats: 1234,
      }),
    }));
    expect(payoutSnapshotService.getLatestExpectedPayoutForAddress).toHaveBeenCalledWith('bc1qtest');
  });

  it('should not query expected PPLNS payout for solo-only address requests', async () => {
    addressSettingsService.getSettings.mockResolvedValue(null);
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 10,
      totalCreditedDifficulty: 100,
      bestSubmissionDifficulty: 0,
    });

    await expect(controller.getClientInfo('bc1qtest', 'solo')).resolves.toEqual(expect.objectContaining({
      expectedPayout: null,
    }));
    expect(payoutSnapshotService.getLatestExpectedPayoutForAddress).not.toHaveBeenCalled();
  });

  it('should expose active database workers for an address', async () => {
    const recent = new Date(Date.now() - 60 * 1000).toISOString();
    const stale = new Date(Date.now() - 60 * 60 * 1000).toISOString();
    clientService.getByAddress.mockResolvedValue([
      {
        id: 'active-client',
        address: 'bc1qtest',
        sessionId: 'active1',
        clientName: 'active-worker',
        payoutMode: 'pplns',
        bestDifficulty: 64,
        hashRate: 1024,
        startTime: '2026-06-08T12:00:00.000Z',
        updatedAt: recent,
      },
      {
        id: 'solo-client',
        address: 'bc1qtest',
        sessionId: 'solo1',
        clientName: 'solo-worker',
        payoutMode: 'solo',
        bestDifficulty: 128,
        hashRate: 2048,
        startTime: '2026-06-08T12:00:00.000Z',
        updatedAt: recent,
      },
      {
        id: 'stale-client',
        address: 'bc1qtest',
        sessionId: 'stale1',
        clientName: 'stale-worker',
        payoutMode: 'pplns',
        bestDifficulty: 256,
        hashRate: 4096,
        startTime: '2026-06-08T12:00:00.000Z',
        updatedAt: stale,
      },
      {
        id: 'idle-client',
        address: 'bc1qtest',
        sessionId: 'idle1',
        clientName: 'idle-worker',
        payoutMode: 'pplns',
        bestDifficulty: 512,
        hashRate: 0,
        startTime: '2026-06-08T12:00:00.000Z',
        updatedAt: recent,
      },
      {
        id: 'deleted-client',
        address: 'bc1qtest',
        sessionId: 'deleted1',
        clientName: 'deleted-worker',
        payoutMode: 'pplns',
        bestDifficulty: 1024,
        hashRate: 8192,
        startTime: '2026-06-08T12:00:00.000Z',
        updatedAt: recent,
        deletedAt: recent,
      },
    ]);
    addressSettingsService.getSettings.mockResolvedValue(null);
    shareAccountingService.getSessionSummaries.mockResolvedValue(new Map());
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 0,
      totalCreditedDifficulty: 0,
      bestSubmissionDifficulty: 0,
    });

    await expect(controller.getClientInfo('bc1qtest', 'pplns')).resolves.toMatchObject({
      workersCount: 1,
      workers: [
        {
          sessionId: 'active1',
          name: 'active-worker',
          payoutMode: 'pplns',
          hashRate: 1024,
        },
      ],
    });
  });
});
