import { ClientService } from './client.service';

describe('ClientService', () => {
    function buildQueryBuilder(rows: any[] = []) {
        const calls: string[] = [];
        const qb: any = {
            calls,
            withDeleted: jest.fn(() => { calls.push('withDeleted'); return qb; }),
            where: jest.fn(() => { calls.push('where'); return qb; }),
            andWhere: jest.fn((clause: string) => { calls.push(`andWhere:${clause}`); return qb; }),
            orderBy: jest.fn(() => qb),
            getMany: jest.fn().mockResolvedValue(rows),
        };
        return qb;
    }

    it('should include soft-deleted rows when collecting address history donors', async () => {
        // `deletedAt` is a @DeleteDateColumn (TrackedEntity), so TypeORM
        // silently filters soft-deleted rows unless withDeleted() is called.
        // Without it every departed identity loses its metadata — its own
        // sessionId, best-difficulty ratchet, start time — and the address
        // page falls back to the share's LANE sessionId with best 0 (live on
        // the test pool 2026-08-10, before this was caught).
        const qb = buildQueryBuilder([{ id: 'c1' }]);
        const repository = { createQueryBuilder: jest.fn(() => qb) };
        const service = new ClientService(repository as any);

        await expect(service.getByAddressIncludingDeleted('bc1qtest')).resolves.toEqual([{ id: 'c1' }]);
        expect(qb.withDeleted).toHaveBeenCalled();
        // …and bounded to the trailing history window rather than every row
        // the address ever produced.
        expect(qb.calls.some(c => c.startsWith('andWhere:') && c.includes('updatedAt'))).toBe(true);
    });

    it('should keep the live-worker query limited to undeleted active rows', async () => {
        const qb = buildQueryBuilder([]);
        const repository = { createQueryBuilder: jest.fn(() => qb) };
        const service = new ClientService(repository as any);

        await service.getByAddress('bc1qtest');
        expect(qb.withDeleted).not.toHaveBeenCalled();
        expect(qb.calls.some(c => c.includes('deletedAt IS NULL'))).toBe(true);
    });
});
