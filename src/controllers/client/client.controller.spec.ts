import { Test, TestingModule } from '@nestjs/testing';

import { AddressSettingsService } from '../../ORM/address-settings/address-settings.service';
import { ClientStatisticsService } from '../../ORM/client-statistics/client-statistics.service';
import { ClientService } from '../../ORM/client/client.service';
import { PayoutSnapshotService } from '../../ORM/payout-snapshot/payout-snapshot.service';
import { ShareAccountingService } from '../../ORM/share-accounting/share-accounting.service';
import { ClientController } from './client.controller';

describe('ClientController', () => {
  let controller: ClientController;
  let clientService: {
    getByAddress: jest.Mock;
    getByAddressIncludingDeleted: jest.Mock;
    getBySessionId: jest.Mock;
  };
  let addressSettingsService: { getSettings: jest.Mock };
  let shareAccountingService: {
    getAddressSummary: jest.Mock;
    getWorkerGroupSummary: jest.Mock;
    getSessionSummary: jest.Mock;
    getSessionSummaries: jest.Mock;
    getAddressWorkerHistory: jest.Mock;
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
                        getByAddressIncludingDeleted: jest.fn().mockResolvedValue([]),
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
                        getAddressWorkerHistory: jest.fn().mockResolvedValue([]),
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
    clientService.getByAddressIncludingDeleted.mockResolvedValue([
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
    clientService.getByAddressIncludingDeleted.mockResolvedValue([
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
    clientService.getByAddressIncludingDeleted.mockResolvedValue([
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
    clientService.getByAddressIncludingDeleted.mockResolvedValue([
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

    // Liveness is the soft-delete + updatedAt window — an idle (hashRate 0)
    // but connected worker is listed (a bursty/fresh session genuinely mining
    // sits at the default 0); the stale and soft-deleted rows are not, and
    // with no share history they contribute nothing else.
    await expect(controller.getClientInfo('bc1qtest', 'pplns')).resolves.toMatchObject({
      workersCount: 2,
      workers: [
        {
          sessionId: 'active1',
          name: 'active-worker',
          payoutMode: 'pplns',
          hashRate: 1024,
          live: true,
        },
        // A zero-hashrate session that is alive (not deleted, recently updated)
        // stays listed: bursty submitters (e.g. time-sliced proxy upstreams)
        // remain at the default hashRate = 0 until a non-zero value is first
        // persisted (zeros are never written).
        {
          sessionId: 'idle1',
          name: 'idle-worker',
          payoutMode: 'pplns',
          hashRate: 0,
          live: true,
        },
      ],
    });
  });

  it('should list a set_payout identity from share history after its connection is gone', async () => {
    // The o927 shape: shares minutes-to-hours old, but no live client row —
    // the virtual presence row died with the lane connection. The workers
    // list must come back from share history, not read "0 workers".
    clientService.getByAddressIncludingDeleted.mockResolvedValue([]);
    shareAccountingService.getAddressWorkerHistory.mockResolvedValue([
      {
        clientId: 'e0e9b1a2-0000-4000-8000-000000000001',
        clientName: 'o927',
        sessionId: 'virt1234',
        payoutMode: 'solo',
        latestShareAt: '2026-08-10T11:50:00.000Z',
        oldestShareAt: '2026-08-09T12:10:00.000Z',
        hashRateLast10Minutes: 0,
        hashRateLastHour: 44_000_000_000_000,
        hashRateLastDay: 10_800_000_000_000,
      },
    ]);
    addressSettingsService.getSettings.mockResolvedValue(null);
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 10_494,
      totalCreditedDifficulty: 500_920_000,
      bestSubmissionDifficulty: 1_370_000_000,
    });

    await expect(controller.getClientInfo('bc1qtest', 'solo')).resolves.toMatchObject({
      workersCount: 1,
      workers: [
        {
          sessionId: 'virt1234',
          name: 'o927',
          payoutMode: 'solo',
          hashRate: 0,
          hashRate1h: 44_000_000_000_000,
          hashRate24h: 10_800_000_000_000,
          live: false,
          startTime: '2026-08-09T12:10:00.000Z',
          lastSeen: '2026-08-10T11:50:00.000Z',
        },
      ],
    });
    expect(shareAccountingService.getAddressWorkerHistory).toHaveBeenCalledWith('bc1qtest', 'solo');
  });

  it('should show the identity own sessionId, not the shared lane sessionId from its shares', async () => {
    // A set_payout identity's accepted shares record the LANE connection's
    // sessionId (its virtual client row carries its own random one), so two
    // renters served by the same lane would otherwise both display that lane's
    // id — and the drill-down route would never resolve. Live on the test pool
    // 2026-08-10: o122 and o119 both showed sessions d8a11c45 / 37959a17.
    clientService.getByAddressIncludingDeleted.mockResolvedValue([
      {
        id: 'a411be2b-0592-4385-b9ab-8b7999963a73',
        address: 'bc1qtest',
        sessionId: 'dde6b045',           // the virtual row's own id
        clientName: 'o122',
        payoutMode: 'solo',
        bestDifficulty: 1_728_643,
        hashRate: 0,
        startTime: '2026-08-10T13:00:00.000Z',
        updatedAt: '2026-08-10T13:32:59.000Z',
        deletedAt: '2026-08-10T13:33:10.000Z',
      },
    ]);
    shareAccountingService.getAddressWorkerHistory.mockResolvedValue([
      {
        clientId: 'a411be2b-0592-4385-b9ab-8b7999963a73',
        clientName: 'o122',
        sessionId: '789dcc8e',           // the lane connection's id
        payoutMode: 'solo',
        latestShareAt: '2026-08-10T13:30:00.000Z',
        oldestShareAt: '2026-08-10T13:00:00.000Z',
        hashRateLast10Minutes: 0,
        hashRateLastHour: 1_840_000_000_000,
        hashRateLastDay: 200_000_000_000,
      },
    ]);
    addressSettingsService.getSettings.mockResolvedValue(null);
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 255,
      totalCreditedDifficulty: 1,
      bestSubmissionDifficulty: 0,
    });

    const response = await controller.getClientInfo('bc1qtest');
    expect(response.workers).toHaveLength(1);
    expect(response.workers[0]).toMatchObject({
      sessionId: 'dde6b045',
      name: 'o122',
      bestDifficulty: '1728643.00',
      live: false,
    });
  });

  it('should merge a live presence row with its share history and keep identity metadata', async () => {
    const recent = new Date(Date.now() - 60 * 1000).toISOString();
    clientService.getByAddressIncludingDeleted.mockResolvedValue([
      {
        id: 'client-a',
        address: 'bc1qtest',
        sessionId: 'sess0001',
        clientName: 'rig',
        payoutMode: 'solo',
        bestDifficulty: 4096,
        hashRate: 0,
        startTime: '2026-08-10T00:00:00.000Z',
        updatedAt: recent,
      },
    ]);
    shareAccountingService.getAddressWorkerHistory.mockResolvedValue([
      {
        clientId: 'client-a',
        clientName: 'rig',
        sessionId: 'sess0001',
        payoutMode: 'solo',
        // Relative, not a fixed instant: the base age-clamps the instantaneous
        // rate against lastSeen, so a hardcoded timestamp makes this test pass
        // only on the day it was written.
        latestShareAt: recent,
        oldestShareAt: '2026-08-10T00:05:00.000Z',
        hashRateLast10Minutes: 5_000_000_000_000,
        hashRateLastHour: 4_000_000_000_000,
        hashRateLastDay: 3_000_000_000_000,
      },
    ]);
    addressSettingsService.getSettings.mockResolvedValue(null);
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 1,
      totalCreditedDifficulty: 1,
      bestSubmissionDifficulty: 0,
    });

    const response = await controller.getClientInfo('bc1qtest');
    // One row, not two: presence supplies identity (start time, best
    // difficulty ratchet), history supplies the windowed rates.
    expect(response.workersCount).toBe(1);
    expect(response.workers[0]).toMatchObject({
      sessionId: 'sess0001',
      name: 'rig',
      bestDifficulty: '4096.00',
      hashRate: 5_000_000_000_000,
      hashRate1h: 4_000_000_000_000,
      hashRate24h: 3_000_000_000_000,
      live: true,
      startTime: '2026-08-10T00:00:00.000Z',
    });
  });

  it('should keep a recently departed session best difficulty on its history row', async () => {
    const recent = new Date(Date.now() - 60 * 1000).toISOString();
    clientService.getByAddressIncludingDeleted.mockResolvedValue([
      {
        id: 'client-b',
        address: 'bc1qtest',
        sessionId: 'sess0002',
        clientName: 'departed',
        payoutMode: 'solo',
        bestDifficulty: 777_000,
        hashRate: 0,
        startTime: '2026-08-10T01:00:00.000Z',
        updatedAt: recent,
        deletedAt: recent,
      },
    ]);
    shareAccountingService.getAddressWorkerHistory.mockResolvedValue([
      {
        clientId: 'client-b',
        clientName: 'departed',
        sessionId: 'sess0002',
        payoutMode: 'solo',
        latestShareAt: '2026-08-10T11:00:00.000Z',
        oldestShareAt: '2026-08-10T01:05:00.000Z',
        hashRateLast10Minutes: 0,
        hashRateLastHour: 0,
        hashRateLastDay: 2_000_000_000_000,
      },
    ]);
    addressSettingsService.getSettings.mockResolvedValue(null);
    shareAccountingService.getAddressSummary.mockResolvedValue({
      totalAcceptedShares: 1,
      totalCreditedDifficulty: 1,
      bestSubmissionDifficulty: 0,
    });

    const response = await controller.getClientInfo('bc1qtest');
    expect(response.workersCount).toBe(1);
    expect(response.workers[0]).toMatchObject({
      name: 'departed',
      bestDifficulty: '777000.00',
      live: false,
      startTime: '2026-08-10T01:00:00.000Z',
    });
  });
});
