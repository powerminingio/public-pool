import { Controller, Get, NotFoundException, Param, Query } from '@nestjs/common';

import { AddressSettingsService } from '../../ORM/address-settings/address-settings.service';
import { ClientStatisticsService } from '../../ORM/client-statistics/client-statistics.service';
import { ClientService } from '../../ORM/client/client.service';
import { PayoutSnapshotService } from '../../ORM/payout-snapshot/payout-snapshot.service';
import { ShareAccountingService } from '../../ORM/share-accounting/share-accounting.service';
import { normalizePayoutMode, PayoutMode } from '../../types/payout-mode';

const DEFAULT_CLIENT_ACTIVE_WINDOW_MS = 30 * 60 * 1000;
// How long a stored entity hashRate stays servable after the session's last
// share. Matches the 10-minute accounting window whose value it substitutes
// for when that window is empty.
const STALE_HASHRATE_WINDOW_MS = 10 * 60 * 1000;

@Controller('client')
export class ClientController {
    private readonly activeWindowMs = this.readPositiveInt('CLIENT_REPORT_ACTIVE_WINDOW_MS', DEFAULT_CLIENT_ACTIVE_WINDOW_MS);

    constructor(
        private readonly clientService: ClientService,
        private readonly clientStatisticsService: ClientStatisticsService,
        private readonly addressSettingsService: AddressSettingsService,
        private readonly shareAccountingService: ShareAccountingService,
        private readonly payoutSnapshotService: PayoutSnapshotService,
    ) { }


    @Get(':address')
    async getClientInfo(@Param('address') address: string, @Query('payoutMode') payoutMode?: string) {
        const mode = this.getRequestedPayoutMode(payoutMode);
        const workers = await this.getAddressWorkers(address, mode);
        const sessionSummaries = await this.shareAccountingService.getSessionSummaries(workers.map(worker => worker.clientId));

        const addressSettings = await this.addressSettingsService.getSettings(address, false);
        const bestDifficulty = addressSettings?.bestDifficulty ?? workers.reduce((best, worker) => {
            return Math.max(best, Number(worker.bestDifficulty ?? 0));
        }, 0);
        const accountingSummary = mode == null
            ? await this.shareAccountingService.getAddressSummary(address)
            : await this.shareAccountingService.getAddressSummary(address, mode);
        const accounting = this.withBestSubmissionDifficulty(accountingSummary, bestDifficulty);
        const expectedPayout = mode === 'solo'
            ? null
            : await this.payoutSnapshotService.getLatestExpectedPayoutForAddress(address);

        const response = {
            bestDifficulty,
            workersCount: workers.length,
            accounting,
            expectedPayout,
            workers: await Promise.all(
                workers.map(async (worker) => {
                    const sessionSummary = sessionSummaries.get(worker.clientId);
                    const bestDifficulty = Math.max(
                        Number(worker.bestDifficulty ?? 0),
                        Number(sessionSummary?.bestSubmissionDifficulty ?? 0),
                    );
                    const lastSeen = sessionSummary?.latestShareAt ?? worker.lastSeen;
                    // The accounting window normalizes to 0 when empty (never
                    // null), so "no usable rate" is any non-positive value —
                    // only a positive window rate beats the entity fallback.
                    const accountingHashRate = Number(sessionSummary?.hashRateLast10Minutes ?? 0);
                    return {
                        sessionId: worker.sessionId,
                        name: worker.clientName,
                        payoutMode: worker.payoutMode,
                        bestDifficulty: bestDifficulty.toFixed(2),
                        hashRate: Number.isFinite(accountingHashRate) && accountingHashRate > 0
                            ? accountingHashRate
                            : this.freshHashRate(worker.hashRate, lastSeen),
                        hashRate1h: worker.hashRate1h,
                        hashRate24h: worker.hashRate24h,
                        live: worker.live,
                        startTime: worker.startTime,
                        lastSeen
                    };
                })
            )
        }
        return response;
    }

    @Get(':address/chart')
    async getClientInfoChart(@Param('address') address: string) {
        return await this.clientStatisticsService.getChartDataForAddress(address);
    }

    @Get(':address/chart/payout-modes')
    async getClientInfoChartByPayoutMode(@Param('address') address: string, @Query('payoutMode') payoutMode?: string) {
        const mode = this.getRequestedPayoutMode(payoutMode);
        return await this.clientStatisticsService.getChartDataForAddressByPayoutMode(address, mode);
    }

    @Get(':address/:workerName')
    async getWorkerGroupInfo(@Param('address') address: string, @Param('workerName') workerName: string, @Query('payoutMode') payoutMode?: string) {
        const mode = this.getRequestedPayoutMode(payoutMode);
        const addressWorkers = await this.getAddressWorkers(address, mode);
        const workers = addressWorkers
            .filter(worker => worker.clientName === workerName);

        const bestDifficulty = workers.reduce((pre, cur, idx, arr) => {
            if (cur.bestDifficulty > pre) {
                return cur.bestDifficulty;
            }
            return pre;
        }, 0);

        const chartData = await this.clientStatisticsService.getChartDataForGroup(address, workerName);
        const chartDataByPayoutMode = await this.clientStatisticsService.getChartDataForGroupByPayoutMode(address, workerName, mode);
        const accountingSummary = mode == null
            ? await this.shareAccountingService.getWorkerGroupSummary(address, workerName)
            : await this.shareAccountingService.getWorkerGroupSummary(address, workerName, mode);
        const accounting = this.withBestSubmissionDifficulty(accountingSummary, bestDifficulty);
        const response = {

            name: workerName,
            bestDifficulty: Math.floor(bestDifficulty),
            payoutModes: [...new Set(workers.map(worker => worker.payoutMode))],
            accounting,
            chartData: chartData,
            chartDataByPayoutMode,

        }
        return response;
    }

    @Get(':address/:workerName/:sessionId')
    async getWorkerInfo(@Param('address') address: string, @Param('workerName') workerName: string, @Param('sessionId') sessionId: string, @Query('payoutMode') payoutMode?: string) {
        const mode = this.getRequestedPayoutMode(payoutMode);
        const addressWorkers = await this.getAddressWorkers(address, mode);
        const presenceWorker = addressWorkers
            .find(worker => worker.clientName === workerName && worker.sessionId === sessionId);
        const worker = presenceWorker == null
            ? await this.clientService.getBySessionId(address, workerName, sessionId)
            : {
                id: presenceWorker.clientId,
                sessionId: presenceWorker.sessionId,
                clientName: presenceWorker.clientName,
                bestDifficulty: presenceWorker.bestDifficulty,
                payoutMode: presenceWorker.payoutMode,
                startTime: presenceWorker.startTime,
            };
        if (worker == null) {
            return new NotFoundException();
        }
        const chartData = await this.clientStatisticsService.getChartDataForSession(worker.id);
        const chartDataByPayoutMode = await this.clientStatisticsService.getChartDataForSessionByPayoutMode(worker.id, mode);
        const accounting = this.withBestSubmissionDifficulty(
            mode == null
                ? await this.shareAccountingService.getSessionSummary(worker.id)
                : await this.shareAccountingService.getSessionSummary(worker.id, mode),
            worker.bestDifficulty,
        );

        const response = {
            sessionId: worker.sessionId,
            name: worker.clientName,
            bestDifficulty: Math.floor(worker.bestDifficulty),
            payoutMode: worker.payoutMode,
            accounting,
            chartData: chartData,
            chartDataByPayoutMode,
            startTime: worker.startTime
        }
        return response;
    }

    private withBestSubmissionDifficulty<T extends { bestSubmissionDifficulty?: number }>(
        accounting: T,
        fallbackBestDifficulty: unknown,
    ): T {
        const existing = Number(accounting?.bestSubmissionDifficulty ?? 0);
        const fallback = Number(fallbackBestDifficulty ?? 0);

        if (!Number.isFinite(fallback) || fallback <= existing) {
            return accounting;
        }

        return {
            ...accounting,
            bestSubmissionDifficulty: fallback,
        };
    }

    /**
     * The stored entity hashRate is computed share-event-side (persisted at
     * share arrival), so between shares nothing updates it: an idle session
     * otherwise serves its last in-burst estimate forever (observed live: a
     * time-sliced proxy upstream reading 2.8 TH/s an hour after its last
     * share; always-on miners never expose this because shares keep it
     * fresh). Serve it only while the session's last share is inside the
     * window it substitutes for; report 0 once older — "Last Seen" already
     * tells the rest of the story.
     *
     * NOTE — to consider for upstream: public-pool master has the same
     * artifact through a different path (getHashRateForSession divides the
     * newest stat buckets without checking their age against now), so the
     * same age clamp applies there.
     */
    private freshHashRate(hashRate: unknown, lastSeen: unknown): number {
        const lastSeenMs = lastSeen == null ? NaN : new Date(lastSeen as string | number | Date).getTime();
        if (!Number.isFinite(lastSeenMs) || Date.now() - lastSeenMs > STALE_HASHRATE_WINDOW_MS) {
            return 0;
        }
        const rate = Number(hashRate ?? 0);
        return Number.isFinite(rate) ? rate : 0;
    }

    private getRequestedPayoutMode(payoutMode?: string): PayoutMode | undefined {
        if (payoutMode == null || payoutMode === 'all') {
            return undefined;
        }
        return normalizePayoutMode(payoutMode);
    }

    /**
     * The address's workers: every LIVE connection/presence row, plus every
     * payout identity with credited work in the trailing day (share history).
     * The union is what makes the list survive connection churn — a
     * `set_payout` identity's virtual presence row is soft-deleted with the
     * lane connection that served it, and a bursty time-sliced session can
     * sit idle past the presence window while its shares are minutes old.
     * Share history cannot vanish that way; presence rows still surface a
     * freshly-connected worker that has no share yet. Client rows (including
     * recently soft-deleted ones, purged a day after deletion — the same
     * horizon as the history window) contribute the identity metadata the
     * rollup doesn't carry: the best-difficulty ratchet and start time.
     */
    private async getAddressWorkers(address: string, payoutMode?: PayoutMode) {
        const [clientRows, history] = await Promise.all([
            this.clientService.getByAddressIncludingDeleted(address),
            this.shareAccountingService.getAddressWorkerHistory(address, payoutMode),
        ]);
        const activeSince = Date.now() - this.activeWindowMs;
        // No hashRate > 0 gate in liveness: a session's hashRate is computed
        // only once its share cache spans >60s, and zero/negative values are
        // never persisted — so a bursty (e.g. time-sliced proxy) or freshly
        // connected worker sits at the default 0 while genuinely mining.
        // Liveness is the soft-delete + updatedAt window; display falls back
        // to share-accounting rates.
        const isLive = (worker: { deletedAt?: Date | null; updatedAt?: Date | null }) => {
            if (worker.deletedAt != null) {
                return false;
            }
            const updatedAt = worker.updatedAt == null ? 0 : new Date(worker.updatedAt).getTime();
            return Number.isFinite(updatedAt) && updatedAt > activeSince;
        };
        const rowsById = new Map(clientRows.map(worker => [worker.id, worker]));

        const merged = new Map<string, {
            clientId: string;
            address: string;
            clientName: string;
            sessionId: string;
            payoutMode: string;
            userAgent: string | null;
            startTime: Date | string | null;
            lastSeen: Date | string | null;
            hashRate: number;
            hashRate1h: number;
            hashRate24h: number;
            bestDifficulty: number;
            live: boolean;
        }>();

        for (const row of history) {
            const client = rowsById.get(row.clientId);
            merged.set(`${row.clientId}:${row.payoutMode}`, {
                clientId: row.clientId,
                address,
                clientName: row.clientName,
                // The client row's own sessionId wins: a set_payout identity's
                // shares record the shared LANE connection's id, so using the
                // share's id would show one lane's id under every renter it
                // served (and never resolve a drill-down route).
                sessionId: client?.sessionId ?? row.sessionId,
                payoutMode: row.payoutMode,
                userAgent: client?.userAgent ?? null,
                startTime: client?.startTime ?? row.oldestShareAt,
                lastSeen: row.latestShareAt ?? client?.updatedAt ?? null,
                hashRate: row.hashRateLast10Minutes,
                hashRate1h: row.hashRateLastHour,
                hashRate24h: row.hashRateLastDay,
                bestDifficulty: Number(client?.bestDifficulty ?? 0),
                live: client != null && isLive(client),
            });
        }

        for (const worker of clientRows) {
            if (!isLive(worker)) {
                continue; // history-only donor; listed above iff it has shares
            }
            if (payoutMode != null && worker.payoutMode !== payoutMode) {
                continue;
            }
            const key = `${worker.id}:${worker.payoutMode}`;
            const fromHistory = merged.get(key);
            merged.set(key, {
                clientId: worker.id,
                address: worker.address,
                clientName: worker.clientName,
                sessionId: worker.sessionId,
                payoutMode: worker.payoutMode,
                userAgent: worker.userAgent,
                startTime: worker.startTime,
                lastSeen: fromHistory?.lastSeen ?? worker.updatedAt,
                hashRate: fromHistory?.hashRate ?? Number(worker.hashRate ?? 0),
                hashRate1h: fromHistory?.hashRate1h ?? 0,
                hashRate24h: fromHistory?.hashRate24h ?? 0,
                bestDifficulty: Number(worker.bestDifficulty ?? 0),
                live: true,
            });
        }

        return [...merged.values()].sort((a, b) => {
            const at = a.lastSeen == null ? 0 : new Date(a.lastSeen).getTime();
            const bt = b.lastSeen == null ? 0 : new Date(b.lastSeen).getTime();
            return bt - at;
        });
    }

    private readPositiveInt(name: string, defaultValue: number): number {
        const value = Number(process.env[name]);
        if (Number.isInteger(value) && value > 0) {
            return value;
        }
        return defaultValue;
    }
}
