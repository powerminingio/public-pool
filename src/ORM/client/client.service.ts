import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { In, Repository } from 'typeorm';

import { ClientEntity } from './client.entity';

const DEFAULT_CLIENT_ACTIVE_WINDOW_MS = 30 * 60 * 1000;
/// How far back departed sessions stay available as identity donors for the
/// address page — the trailing day the share history covers, plus an hour so
/// a session right at the boundary doesn't lose its metadata.
const CLIENT_HISTORY_WINDOW_MS = 25 * 60 * 60 * 1000;

@Injectable()
export class ClientService {
    private readonly activeWindowMs = this.readPositiveInt('CLIENT_REPORT_ACTIVE_WINDOW_MS', DEFAULT_CLIENT_ACTIVE_WINDOW_MS);

    constructor(
        @InjectRepository(ClientEntity)
        private clientRepository: Repository<ClientEntity>
    ) {

    }

    public async insert(partialClient: Partial<ClientEntity>): Promise<ClientEntity> {
        const insertResult = await this.clientRepository.insert(partialClient);

        const client = {
            ...partialClient,
            ...insertResult.generatedMaps[0]
        };

        return client as ClientEntity;
    }

    public async delete(id: string) {
        return await this.clientRepository.softDelete({ id });
    }

    public async deleteOldClients() {

        const oneDayAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);

        return await this.clientRepository
            .createQueryBuilder()
            .delete()
            .from(ClientEntity)
            .where('deletedAt < :deletedAt', { deletedAt: oneDayAgo })
            .execute();

    }

    public async updateBestDifficulty(id: string, bestDifficulty: number) {
        return await this.clientRepository.update({ id }, { bestDifficulty });
    }

    public async updateBestDifficultyIfHigher(id: string, bestDifficulty: number) {
        return await this.clientRepository
            .createQueryBuilder()
            .update(ClientEntity)
            .set({ bestDifficulty })
            .where('id = :id', { id })
            .andWhere('"bestDifficulty" < :bestDifficulty', { bestDifficulty })
            .execute();
    }

    public async updateHashRate(id: string, hashRate: number, updatedAt = new Date()) {
        return await this.clientRepository.update({ id }, { hashRate, updatedAt, deletedAt: null });
    }

    // Refresh a row's liveness (updatedAt window + un-soft-delete) without
    // touching its hashRate. Used for virtual worker presences maintained by
    // mining.set_payout, which have no per-connection hashrate of their own —
    // their displayed rate comes from share accounting.
    public async heartbeat(id: string, updatedAt = new Date()) {
        return await this.clientRepository.update({ id }, { updatedAt, deletedAt: null });
    }

    public async connectedClientCount(): Promise<number> {
        return await this.clientRepository.count();
    }

    public async getActiveIds(ids: string[]): Promise<Set<string>> {
        if (ids.length === 0) {
            return new Set();
        }

        const clients = await this.clientRepository.find({
            select: {
                id: true,
            },
            where: {
                id: In(ids),
            },
        });

        return new Set(clients.map(client => client.id));
    }

    public async getByAddress(address: string): Promise<ClientEntity[]> {
        const activeSince = new Date(Date.now() - this.activeWindowMs);
        return await this.clientRepository
            .createQueryBuilder('client')
            .where('client.address = :address', { address })
            .andWhere('client.deletedAt IS NULL')
            .andWhere('client.updatedAt > :activeSince', { activeSince })
            .orderBy('client.updatedAt', 'DESC')
            .getMany();
    }

    /**
     * Like [getByAddress] but including soft-deleted and idle rows — the
     * identity metadata (own sessionId, best-difficulty ratchet, start time,
     * user agent) of sessions whose connection is gone.
     *
     * `withDeleted()` is load-bearing: `deletedAt` is a `@DeleteDateColumn`
     * (TrackedEntity), so TypeORM silently excludes soft-deleted rows
     * otherwise — the query returns live rows only and every departed
     * identity loses its metadata with no error to show for it.
     *
     * Bounded to the same trailing day the share history covers, so this
     * doesn't depend on the deletion purge having run.
     */
    public async getByAddressIncludingDeleted(address: string): Promise<ClientEntity[]> {
        const since = new Date(Date.now() - CLIENT_HISTORY_WINDOW_MS);
        return await this.clientRepository
            .createQueryBuilder('client')
            .withDeleted()
            .where('client.address = :address', { address })
            .andWhere('client.updatedAt > :since', { since })
            .orderBy('client.updatedAt', 'DESC')
            .getMany();
    }


    public async getByName(address: string, clientName: string): Promise<ClientEntity[]> {
        return await this.clientRepository.find({
            where: {
                address,
                clientName
            }
        })
    }

    public async getBySessionId(address: string, clientName: string, sessionId: string): Promise<ClientEntity> {
        return await this.clientRepository.findOne({
            where: {
                address,
                clientName,
                sessionId
            }
        })
    }

    public async deleteAll() {
        return await this.clientRepository
            .createQueryBuilder()
            .delete()
            .from(ClientEntity)
            .where('"deletedAt" IS NULL')
            .execute();
    }

    // public async getUserAgents() {
    //     const result = await this.clientRepository.createQueryBuilder('client')
    //         .select('client.userAgent as "userAgent"')
    //         .addSelect('COUNT(client.userAgent)', 'count')
    //         .addSelect('MAX(client.bestDifficulty)', 'bestDifficulty')
    //         .addSelect('SUM(client.hashRate)', 'totalHashRate')
    //         .groupBy('client.userAgent')
    //         .orderBy('"totalHashRate"', 'DESC')
    //         .getRawMany();
    //     return result;
    // }

    private readPositiveInt(name: string, defaultValue: number): number {
        const value = Number(process.env[name]);
        if (Number.isInteger(value) && value > 0) {
            return value;
        }
        return defaultValue;
    }

}
