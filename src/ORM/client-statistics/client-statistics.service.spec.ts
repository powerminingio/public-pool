import { ClientStatisticsService } from './client-statistics.service';

describe('ClientStatisticsService chart gapfill', () => {

    afterEach(() => {
        jest.useRealTimers();
        jest.restoreAllMocks();
    });

    function serviceReturning(rows: unknown[]): ClientStatisticsService {
        const dataSource = {
            query: jest.fn().mockResolvedValue(rows),
        };
        return new ClientStatisticsService(dataSource as any);
    }

    function row(label: string, shares: number) {
        return {
            label: new Date(label),
            data: String(Math.round((shares * 4294967296) / 600)),
            shares,
            acceptedCount: 1,
        };
    }

    it('fills interior gaps with zero points', async () => {
        jest.useFakeTimers().setSystemTime(new Date('2026-07-14T09:05:00Z'));
        const service = serviceReturning([
            row('2026-07-14T08:20:00Z', 5000),
            row('2026-07-14T08:50:00Z', 7000),
        ]);

        const chart = await service.getChartDataForAddress('bc1qtest');

        expect(chart.map(point => [point.label, point.shares])).toEqual([
            ['2026-07-14T08:20:00.000Z', 5000],
            ['2026-07-14T08:30:00.000Z', 0],
            ['2026-07-14T08:40:00.000Z', 0],
            ['2026-07-14T08:50:00.000Z', 7000],
        ]);
        expect(chart[1]).toEqual({
            label: '2026-07-14T08:30:00.000Z',
            data: '0',
            shares: 0,
            acceptedCount: 0,
        });
    });

    it('does not fill before the first bucket in the window', async () => {
        jest.useFakeTimers().setSystemTime(new Date('2026-07-14T09:05:00Z'));
        const service = serviceReturning([
            row('2026-07-14T08:50:00Z', 7000),
        ]);

        const chart = await service.getChartDataForAddress('bc1qtest');

        expect(chart[0].label).toBe('2026-07-14T08:50:00.000Z');
        expect(chart).toHaveLength(1);
    });

    it('fills a trailing gap up to, and excluding, the current bucket', async () => {
        jest.useFakeTimers().setSystemTime(new Date('2026-07-14T09:25:00Z'));
        const service = serviceReturning([
            row('2026-07-14T08:50:00Z', 7000),
        ]);

        const chart = await service.getChartDataForAddress('bc1qtest');

        expect(chart.map(point => [point.label, point.shares])).toEqual([
            ['2026-07-14T08:50:00.000Z', 7000],
            ['2026-07-14T09:00:00.000Z', 0],
            ['2026-07-14T09:10:00.000Z', 0],
        ]);
    });

    it('returns an empty chart unchanged', async () => {
        jest.useFakeTimers().setSystemTime(new Date('2026-07-14T09:05:00Z'));
        const service = serviceReturning([]);

        await expect(service.getChartDataForAddress('bc1qtest')).resolves.toEqual([]);
    });

    it('fills payout-mode series independently and keeps label/mode ordering', async () => {
        jest.useFakeTimers().setSystemTime(new Date('2026-07-14T09:05:00Z'));
        const service = serviceReturning([
            { ...row('2026-07-14T08:30:00Z', 5000), payoutMode: 'solo' },
            { ...row('2026-07-14T08:50:00Z', 6000), payoutMode: 'solo' },
            { ...row('2026-07-14T08:40:00Z', 1000), payoutMode: 'pplns' },
        ]);

        const chart = await service.getChartDataForAddressByPayoutMode('bc1qtest');

        expect(chart.map(point => [point.label, point.payoutMode, point.shares])).toEqual([
            ['2026-07-14T08:30:00.000Z', 'solo', 5000],
            ['2026-07-14T08:40:00.000Z', 'pplns', 1000],
            ['2026-07-14T08:40:00.000Z', 'solo', 0],
            ['2026-07-14T08:50:00.000Z', 'pplns', 0],
            ['2026-07-14T08:50:00.000Z', 'solo', 6000],
        ]);
    });
});
