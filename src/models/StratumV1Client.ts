import { ConfigService } from '@nestjs/config';
import * as bitcoinjs from 'bitcoinjs-lib';
import { plainToInstance } from 'class-transformer';
import { validate, ValidationError, ValidatorOptions } from 'class-validator';
import * as crypto from 'crypto';
import { Socket } from 'net';
import { filter, firstValueFrom } from 'rxjs';
import { clearInterval } from 'timers';

import { AddressSettingsService } from '../ORM/address-settings/address-settings.service';
import { BlocksService } from '../ORM/blocks/blocks.service';
import { ClientEntity } from '../ORM/client/client.entity';
import { ClientService } from '../ORM/client/client.service';
import { PayoutSnapshotService } from '../ORM/payout-snapshot/payout-snapshot.service';
import { ShareAccountingService } from '../ORM/share-accounting/share-accounting.service';
import { BitcoinRpcService } from '../services/bitcoin-rpc.service';
import { NotificationService } from '../services/notification.service';
import { RedisMessagingService } from '../services/redis-messaging.service';
import {
    createPayoutOutputIdentity,
    IJobSubmissionContext,
    IJobTemplate,
    StratumV1JobsService,
} from '../services/stratum-v1-jobs.service';
import { DifficultyUtils } from '../utils/difficulty.utils';
import { hash256 } from '../utils/hash.utils';
import { PayoutMode } from '../types/payout-mode';
import { eRequestMethod } from './enums/eRequestMethod';
import { eResponseMethod } from './enums/eResponseMethod';
import { eStratumErrorCode } from './enums/eStratumErrorCode';
import { AddressObject, MiningJob } from './MiningJob';
import { AuthorizationMessage } from './stratum-messages/AuthorizationMessage';
import { ConfigurationMessage } from './stratum-messages/ConfigurationMessage';
import { MiningSubmitMessage } from './stratum-messages/MiningSubmitMessage';
import { SetPayoutMessage } from './stratum-messages/SetPayoutMessage';
import { StratumErrorMessage } from './stratum-messages/StratumErrorMessage';
import { SubscriptionMessage } from './stratum-messages/SubscriptionMessage';
import { EXTRANONCE1_SIZE_BYTES } from './stratum.constants';
import { SuggestDifficulty } from './stratum-messages/SuggestDifficultyMessage';
import { StratumV1ClientStatistics } from './StratumV1ClientStatistics';

const TRUE_DIFF_ONE = 2.695953529101131e67;
const BLOCKED_USER_AGENT_LOG_INTERVAL_MS = 60 * 1000;
const VALIDATION_ERROR_LOG_INTERVAL_MS = 60 * 1000;
const DEFAULT_MIN_DIFFICULTY = 0.001;
const DEFAULT_CLIENT_HASHRATE_PERSIST_INTERVAL_MS = 60 * 1000;
const DEFAULT_SUBMISSION_DEDUP_TTL_MS = 5 * 60 * 1000;
const DEFAULT_SUBMISSION_DEDUP_MAX_ENTRIES = 10_000;
const DEFAULT_MAX_SOCKET_BUFFER_BYTES = 256 * 1024;
const VERSION_ROLLING_MASK = 0x1fffe000;
// How many payout identities a single connection remembers having been served.
// Only a bound on memory: how long a job stays submittable is the jobs service's
// own retention (5 min by default), after which the job is gone regardless.
const SERVED_PAYOUT_IDENTITY_MAX_ENTRIES = 1_000;

export interface MiningJobBroadcastResult {
    status: 'written' | 'backpressured' | 'skipped' | 'closed' | 'error';
    bytes: number;
    bufferedBytes: number;
    preStaged?: boolean;
}

export class StratumV1Client {
    private static blockedUserAgentLogState = new Map<string, { nextLogAt: number, suppressed: number }>();
    private static validationErrorLogState = new Map<string, { nextLogAt: number, suppressed: number, sample: string }>();

    public clientSubscription: SubscriptionMessage;
    private clientConfiguration: ConfigurationMessage;
    private clientAuthorization: AuthorizationMessage;
    // The address this connection's coinbase currently pays. Initialized to the
    // authorized address; may be changed at runtime via mining.set_payout (the
    // connection's extranonce1 stays stable). A connection that never sends
    // set_payout keeps this equal to clientAuthorization.address — i.e. unchanged
    // behaviour.
    private currentPayoutAddress: string;
    // Every payout identity this connection has actually been served a job for,
    // most-recently-served last. A submit is only accepted against a job whose
    // identity is in here (see isOwnedSubmissionContext) — so after a
    // mining.set_payout switch a late in-flight share for the PREVIOUS address
    // still validates, while a connection can never submit against an identity
    // it was never served. Bounded (LRU) so a client looping set_payout cannot
    // grow it without end.
    private servedPayoutIdentities = new Set<string>();
    // The worker label of the payout identity, alongside currentPayoutAddress.
    // Initialized to the authorized worker; a mining.set_payout with the
    // optional second param overrides it, and one without it falls back to the
    // authorized worker (the pre-worker-param behaviour).
    private currentPayoutWorker: string;
    // jobId -> the worker label THIS connection served that job under. Jobs are
    // shared between connections by payout identity on this base, so the label
    // (a per-connection notion — two rigs on one address routinely carry
    // different worker names) cannot live on the MiningJob itself. Recording it
    // per connection at serve time keeps a late in-flight submit after a
    // set_payout switch attributed to the identity the work was built for.
    // Bounded (LRU) exactly like servedPayoutIdentities.
    private servedJobWorkers = new Map<string, string>();
    private clientSuggestedDifficulty: SuggestDifficulty;
    private backgroundWork: NodeJS.Timeout[] = [];
    private readonly socketDataHandler: (data: Buffer) => void;
    private destroyPromise: Promise<void> | null = null;

    private statistics: StratumV1ClientStatistics;
    private stratumInitialized = false;
    private usedSuggestedDifficulty = false;
    private sessionDifficulty: number = 100000;
    private sessionDifficultyTarget: Buffer = DifficultyUtils.difficultyToTarget(this.sessionDifficulty);

    private clientEntity: ClientEntity;
    private creatingEntity: Promise<void>;
    // Virtual worker presences maintained via mining.set_payout: one ClientEntity
    // row per (payout address, worker) this connection has served that differs
    // from the authorized identity — those identities have no connection of their
    // own, so without these rows the address page's workers list would be empty.
    // Keyed address\0worker; the map holds the insert promise so a submit racing
    // the row creation awaits the same row instead of duplicating it.
    private virtualWorkerEntities = new Map<string, Promise<ClientEntity>>();

    public extraNonceAndSessionId: string;
    public sessionStart: Date;
    //public noFee: boolean;
    //public hashRate: number = 0;

    private buffer: string = '';
    private connectionClosed = false;
    private lastSentMiningJobTimestamp: number = null;
    private lastSentMiningJobSignature: string = null;
    private lastSentMiningTipKey: string = null;
    private lastSentMiningJobType: 'full' | 'empty' | null = null;
    private lastHashRatePersistedAt = 0;
    private readonly network: bitcoinjs.Network;
    private readonly maxSocketBufferBytes: number;

    private miningSubmissionHashes = new Map<string, number>();

    constructor(
        public readonly socket: Socket,
        private readonly stratumV1JobsService: StratumV1JobsService,
        private readonly bitcoinRpcService: BitcoinRpcService,
        private readonly clientService: ClientService,
        private readonly notificationService: NotificationService,
        private readonly blocksService: BlocksService,
        private readonly configService: ConfigService,
        private readonly addressSettingsService: AddressSettingsService,
        private readonly shareAccountingService?: ShareAccountingService,
        private readonly redisMessagingService?: RedisMessagingService,
        private readonly payoutSnapshotService?: PayoutSnapshotService,
        private readonly accountingProtocol: 'sv1' | 'sv1_tls' = 'sv1',
        private readonly payoutMode: PayoutMode = 'solo',
    ) {

        this.socketDataHandler = (data: Buffer) => {
            void this.handleSocketData(data);
        };
        this.socket.on('data', this.socketDataHandler);
        this.network = this.getNetwork();
        this.maxSocketBufferBytes = this.readMaxSocketBufferBytes();


    }

    public async destroy(): Promise<void> {
        if (this.destroyPromise != null) {
            return this.destroyPromise;
        }

        this.destroyPromise = this.destroyInternal();
        return this.destroyPromise;
    }

    private async destroyInternal(): Promise<void> {
        this.connectionClosed = true;
        this.socket.removeListener('data', this.socketDataHandler);
        this.buffer = '';

        for (const work of this.backgroundWork) {
            clearInterval(work);
        }
        this.backgroundWork = [];
        this.miningSubmissionHashes.clear();

        // The connection's own row and the virtual worker presences maintained
        // via set_payout live and die with the connection. Soft-delete them in
        // parallel, tolerating per-row failures: destroy() is awaited from the
        // socket close handler, where a rejection would go unhandled — and a
        // failed soft-delete only leaves a row to age out of the report window.
        const virtualEntities = await Promise.all(
            [...this.virtualWorkerEntities.values()].map((pending) => pending.catch(() => null))
        );
        this.virtualWorkerEntities.clear();
        const rowIds = [this.clientEntity?.id, ...virtualEntities.map((entity) => entity?.id)]
            .filter((id): id is string => id != null);
        const deletions = await Promise.allSettled(rowIds.map((id) => this.clientService.delete(id)));
        for (const deletion of deletions) {
            if (deletion.status === 'rejected') {
                console.warn(`Failed to soft-delete client row on disconnect: ${deletion.reason?.message ?? deletion.reason}`);
            }
        }
    }

    private async handleSocketData(data: Buffer): Promise<void> {
        if (this.connectionClosed || this.socket.destroyed || this.socket.writableEnded) {
            return;
        }

        this.buffer += data.toString();
        const lines = this.buffer.split('\n');
        this.buffer = lines.pop() || ''; // Save the last part of the data (incomplete line) to the buffer

        for (const m of lines.filter(l => l.length > 0)) {
            if (this.connectionClosed || this.socket.destroyed || this.socket.writableEnded) {
                break;
            }
            try {
                await this.handleMessage(m);
            } catch (e) {
                await this.socket.end();
                console.error(e);
            }
        }
    }

    private getRandomHexString() {
        const randomBytes = crypto.randomBytes(EXTRANONCE1_SIZE_BYTES);
        return randomBytes.toString('hex');
    }


    private async handleMessage(message: string) {
        //console.log(`Received from ${this.extraNonceAndSessionId}`, message);

        // Parse the message and check if it's the initial subscription message
        let parsedMessage = null;
        try {
            parsedMessage = JSON.parse(message);
        } catch (e) {
            //console.log("Invalid JSON");
            await this.socket.end();
            return;
        }



        switch (parsedMessage.method) {
            case eRequestMethod.SUBSCRIBE: {
                const subscriptionMessage = plainToInstance(
                    SubscriptionMessage,
                    parsedMessage,
                );

                const validatorOptions: ValidatorOptions = {
                    whitelist: true,
                    //forbidNonWhitelisted: true,
                };

                const errors = await validate(subscriptionMessage, validatorOptions);

                if (errors.length === 0) {
                    if (this.isBlockedUserAgent(subscriptionMessage.userAgent)) {
                        this.logBlockedUserAgent(subscriptionMessage.userAgent);
                        this.closeSocket();
                        return;
                    }

                    if (this.sessionStart == null) {
                        this.sessionStart = new Date();
                        this.statistics = new StratumV1ClientStatistics(this.getMinimumDifficulty());
                        this.extraNonceAndSessionId = this.getRandomHexString();
                        //console.log(`New client ID: : ${this.extraNonceAndSessionId}, ${this.socket.remoteAddress}:${this.socket.remotePort}`);
                    }

                    this.clientSubscription = subscriptionMessage;
                    const success = await this.write(JSON.stringify(this.clientSubscription.response(this.extraNonceAndSessionId)) + '\n');
                    if (!success) {
                        return;
                    }
                } else {
                    console.error('Subscription validation error');
                    const err = new StratumErrorMessage(
                        subscriptionMessage.id,
                        eStratumErrorCode.OtherUnknown,
                        'Subscription validation error',
                        errors).response();
                    console.error(err);
                    const success = await this.write(err);
                    if (!success) {
                        return;
                    }
                }

                break;
            }
            case eRequestMethod.CONFIGURE: {

                const configurationMessage = plainToInstance(
                    ConfigurationMessage,
                    parsedMessage,
                );

                const validatorOptions: ValidatorOptions = {
                    whitelist: true,
                    //forbidNonWhitelisted: true,
                };

                const errors = await validate(configurationMessage, validatorOptions);

                if (errors.length === 0) {
                    this.clientConfiguration = configurationMessage;
                    //const response = this.buildSubscriptionResponse(configurationMessage.id);
                    const success = await this.write(JSON.stringify(this.clientConfiguration.response()) + '\n');
                    if (!success) {
                        return;
                    }

                } else {
                    console.log('Configuration validation error');
                    const err = new StratumErrorMessage(
                        configurationMessage.id,
                        eStratumErrorCode.OtherUnknown,
                        'Configuration validation error',
                        errors).response();
                    console.error(err);
                    const success = await this.write(err);
                    if (!success) {
                        return;
                    }
                }

                break;
            }
            case eRequestMethod.AUTHORIZE: {

                const authorizationMessage = plainToInstance(
                    AuthorizationMessage,
                    parsedMessage,
                );

                const validatorOptions: ValidatorOptions = {
                    whitelist: true,
                    //forbidNonWhitelisted: true,
                };

                const errors = await validate(authorizationMessage, validatorOptions);

                if (errors.length === 0) {
                    this.clientAuthorization = authorizationMessage;
                    this.currentPayoutAddress = authorizationMessage.address;
                    this.currentPayoutWorker = authorizationMessage.worker;
                    if (this.clientSuggestedDifficulty == null && this.clientAuthorization.startingDiff != null && this.clientAuthorization.startingDiff > this.sessionDifficulty) {
                        this.sessionDifficulty = this.clientAuthorization.startingDiff;
                        this.sessionDifficultyTarget = DifficultyUtils.difficultyToTarget(this.sessionDifficulty);
                    }
                    const success = await this.write(JSON.stringify(this.clientAuthorization.response()) + '\n');
                    if (!success) {
                        return;
                    }
                } else {
                    const err = new StratumErrorMessage(
                        authorizationMessage.id,
                        eStratumErrorCode.OtherUnknown,
                        'Authorization validation error',
                        errors).response();
                    //console.log(err);
                    const success = await this.write(err);
                    if (!success) {
                        return;
                    }
                }

                break;
            }
            case eRequestMethod.SUGGEST_DIFFICULTY: {
                if (this.usedSuggestedDifficulty == true) {
                    return;
                }

                const suggestDifficultyMessage = plainToInstance(
                    SuggestDifficulty,
                    parsedMessage
                );

                const validatorOptions: ValidatorOptions = {
                    whitelist: true,
                    //forbidNonWhitelisted: true,
                };

                const errors = await validate(suggestDifficultyMessage, validatorOptions);

                if (errors.length === 0) {

                    this.clientSuggestedDifficulty = suggestDifficultyMessage;
                    this.sessionDifficulty = this.clampDifficulty(suggestDifficultyMessage.suggestedDifficulty);
                    this.sessionDifficultyTarget = DifficultyUtils.difficultyToTarget(this.sessionDifficulty);
                    const success = await this.write(JSON.stringify(this.clientSuggestedDifficulty.response(this.sessionDifficulty)) + '\n');
                    if (!success) {
                        return;
                    }
                    this.usedSuggestedDifficulty = true;
                } else {
                    console.error('Suggest difficulty validation error');
                    const err = new StratumErrorMessage(
                        suggestDifficultyMessage.id,
                        eStratumErrorCode.OtherUnknown,
                        'Suggest difficulty validation error',
                        errors).response();
                    console.error(err);
                    const success = await this.write(err);
                    if (!success) {
                        return;
                    }
                }
                break;
            }
            case eRequestMethod.SUBMIT: {

                if (this.stratumInitialized == false) {
                    //console.log('Submit before initalized');
                    await this.socket.end();
                    return;
                }


                const miningSubmitMessage = plainToInstance(
                    MiningSubmitMessage,
                    parsedMessage,
                );

                const validatorOptions: ValidatorOptions = {
                    whitelist: true,
                    //forbidNonWhitelisted: true,
                };

                const errors = await validate(miningSubmitMessage, validatorOptions);

                if (errors.length === 0 && this.stratumInitialized == true) {
                    const result = await this.handleMiningSubmission(miningSubmitMessage);
                    if (result == true) {
                        const success = await this.write(JSON.stringify(miningSubmitMessage.response()) + '\n');
                        if (!success) {
                            return;
                        }
                    }


                } else {
                    this.logValidationError('Mining Submit validation error', errors);
                    const err = new StratumErrorMessage(
                        miningSubmitMessage.id,
                        eStratumErrorCode.OtherUnknown,
                        'Mining Submit validation error',
                        errors).response();
                    const success = await this.write(err);
                    if (!success) {
                        return;
                    }
                    this.closeSocket();
                    return;
                }
                break;
            }
            case eRequestMethod.SET_PAYOUT: {

                const setPayoutMessage = plainToInstance(
                    SetPayoutMessage,
                    parsedMessage,
                );

                const validatorOptions: ValidatorOptions = {
                    whitelist: true,
                    //forbidNonWhitelisted: true,
                };

                const errors = await validate(setPayoutMessage, validatorOptions);

                if (errors.length === 0) {
                    this.currentPayoutAddress = setPayoutMessage.address;
                    this.currentPayoutWorker = setPayoutMessage.worker ?? this.clientAuthorization?.worker;
                    const success = await this.write(JSON.stringify(setPayoutMessage.response()) + '\n');
                    if (!success) {
                        return;
                    }

                    // If we're already serving work, push a fresh job paying the new
                    // address immediately (stable extranonce1, new coinbase) so the
                    // switch takes effect without waiting for the next template. Same
                    // clean-jobs refresh pattern as checkDifficulty(); the new coinbase
                    // guarantees the work is not byte-identical. Old jobs stay in the
                    // jobs service (5 min) so a late in-flight submit for the previous
                    // address still validates and is attributed correctly.
                    if (this.stratumInitialized) {
                        // Register the presence before serving work for the identity so
                        // a submit racing the switch finds the row (the map is populated
                        // synchronously; the insert itself is awaited after the job push).
                        const presence = this.ensurePayoutPresence();
                        const jobTemplate = await this.getLatestPayoutJobTemplate();
                        const nextTimestamp = Math.max(
                            jobTemplate.block.timestamp,
                            Math.floor(Date.now() / 1000),
                            (this.lastSentMiningJobTimestamp ?? 0) + 1
                        );
                        const refreshedJobTemplate: IJobTemplate = {
                            ...jobTemplate,
                            block: Object.assign(new bitcoinjs.Block(), jobTemplate.block, {
                                timestamp: nextTimestamp
                            }),
                            blockData: { ...jobTemplate.blockData, clearJobs: true }
                        };
                        this.broadcastMiningJob(refreshedJobTemplate, true);
                        await presence;
                    }
                } else {
                    console.error('Set payout validation error');
                    const err = new StratumErrorMessage(
                        setPayoutMessage.id,
                        eStratumErrorCode.OtherUnknown,
                        'Set payout validation error',
                        errors).response();
                    console.error(err);
                    const success = await this.write(err);
                    if (!success) {
                        return;
                    }
                }

                break;
            }
            // default: {
            //     console.log("Invalid message");
            //     console.log(parsedMessage);
            //     await this.socket.end();
            //     return;
            // }
        }


        if (this.clientSubscription != null
            && this.clientAuthorization != null
            && this.stratumInitialized == false) {

            this.initStratum();

        }
    }

    private async initStratum() {
        this.socket.setTimeout(0);

        if (this.isBlockedUserAgent(this.clientSubscription.userAgent)) {
            this.logBlockedUserAgent(this.clientSubscription.userAgent);
            this.closeSocket();
            return;
        }

        if (this.clientSuggestedDifficulty == null) {
            //console.log(`Setting difficulty to ${this.sessionDifficulty}`)
            const setDifficulty = JSON.stringify(new SuggestDifficulty().response(this.sessionDifficulty));
            const success = await this.write(setDifficulty + '\n');
            if (!success) {
                return;
            }
        }

        await this.ensureClientEntity();
        this.stratumInitialized = true;
        const latestJobTemplate = await this.getLatestPayoutJobTemplate();
        this.broadcastMiningJob(latestJobTemplate);
        const latestPrestage = this.stratumV1JobsService.getLatestPrestageJobTemplate(this.payoutMode);
        if (latestPrestage != null) {
            this.preStageMiningJob(latestPrestage);
        }

        this.backgroundWork.push(
            setInterval(async () => {
                await this.checkDifficulty();
            }, 60 * 1000)
        );

        // this.backgroundWork.push(
        //     setInterval(async () => {
        //         await this.statistics.saveShares(this.clientEntity);
        //     }, 60 * 1000)
        // );
    }

    public isReadyForMiningJobs(): boolean {
        return this.stratumInitialized
            && !this.connectionClosed
            && !this.socket.destroyed
            && !this.socket.writableEnded;
    }

    public preStageMiningJob(jobTemplate: IJobTemplate): boolean {
        if (!this.isReadyForMiningJobs()
            || jobTemplate.blockData.jobType !== 'empty'
            || jobTemplate.blockData.payoutMode !== this.payoutMode) {
            return false;
        }
        const payoutInformation = this.getPayoutInformation(
            jobTemplate,
            this.getCurrentPayoutAddress(),
        );
        if (payoutInformation == null) {
            return false;
        }
        const payoutIdentity = this.getPayoutIdentity(jobTemplate);
        return this.stratumV1JobsService.preStageJob(
            this.network,
            payoutInformation,
            jobTemplate,
            payoutIdentity,
            this.payoutMode,
        ) != null;
    }

    public broadcastMiningJob(jobTemplate: IJobTemplate, force = false): MiningJobBroadcastResult {
        if (!this.isReadyForMiningJobs()) {
            return { status: 'skipped', bytes: 0, bufferedBytes: this.socket.writableLength ?? 0 };
        }
        if (jobTemplate.blockData.payoutMode !== 'all'
            && jobTemplate.blockData.payoutMode !== this.payoutMode) {
            return { status: 'skipped', bytes: 0, bufferedBytes: this.socket.writableLength ?? 0 };
        }

        const signature = [
            jobTemplate.blockData.id,
            jobTemplate.block.timestamp,
            jobTemplate.blockData.clearJobs,
        ].join(':');
        if (!force && signature === this.lastSentMiningJobSignature) {
            return { status: 'skipped', bytes: 0, bufferedBytes: this.socket.writableLength ?? 0 };
        }
        if (!force
            && jobTemplate.blockData.jobType === 'empty'
            && this.lastSentMiningJobType === 'empty'
            && this.lastSentMiningTipKey === jobTemplate.blockData.tipKey) {
            return { status: 'skipped', bytes: 0, bufferedBytes: this.socket.writableLength ?? 0 };
        }

        const maximumBufferedBytes = this.maxSocketBufferBytes;
        const bufferedBeforeBuild = this.socket.writableLength ?? 0;
        if (bufferedBeforeBuild >= maximumBufferedBytes) {
            this.closeSocket();
            return { status: 'closed', bytes: 0, bufferedBytes: bufferedBeforeBuild };
        }

        const payoutInformation = this.getPayoutInformation(jobTemplate, this.getCurrentPayoutAddress());
        if (payoutInformation == null) {
            return { status: 'skipped', bytes: 0, bufferedBytes: this.socket.writableLength ?? 0 };
        }
        // const devFeeAddress = this.configService.get('DEV_FEE_ADDRESS');
        // //50Th/s
        // this.noFee = false;
        // if (this.clientEntity) {
        //     // 250Gh/s
        //     if(this.hashRate < 250000000000){
        //         this.statistics.targetSubmitShareEveryNSeconds = 10;
        //     }
        //     this.noFee = this.hashRate != 0 && this.hashRate < 50000000000000;
        // }
        // if (this.noFee || devFeeAddress == null || devFeeAddress.length < 1) {
        //     payoutInformation = [
        //         { address: this.clientAuthorization.address, percent: 100 }
        //     ];

        // } else {
        //     payoutInformation = [
        //         { address: devFeeAddress, percent: 1.5 },
        //         { address: this.clientAuthorization.address, percent: 98.5 }
        //     ];
        // }

        const payoutIdentity = this.getPayoutIdentity(jobTemplate);
        const preStagedJob = (
            jobTemplate.blockData.jobType === 'empty'
                ? this.stratumV1JobsService.activatePreStagedJob(
                    jobTemplate,
                    payoutIdentity,
                    this.payoutMode,
                )
                : null
        );
        const job = preStagedJob ?? this.stratumV1JobsService.getOrCreateJob(
            this.network,
            payoutInformation,
            jobTemplate,
            payoutIdentity,
            this.payoutMode,
        );
        const payload = job.responseBuffer(
            jobTemplate,
            this.stratumV1JobsService.getNotifyHeaderFields?.(jobTemplate),
        );
        const bufferedBeforeWrite = this.socket.writableLength ?? 0;
        if (bufferedBeforeWrite >= maximumBufferedBytes
            || payload.length >= maximumBufferedBytes - bufferedBeforeWrite) {
            this.closeSocket();
            return {
                status: 'closed',
                bytes: 0,
                bufferedBytes: bufferedBeforeWrite,
                ...(preStagedJob == null ? {} : { preStaged: true }),
            };
        }
        try {
            const accepted = this.socket.write(payload);
            // The job is on the wire, so this identity's jobs are now submittable
            // by this connection — including after a later mining.set_payout moves
            // the connection on to another address.
            this.rememberServedPayoutIdentity(job.ownership?.payoutIdentity ?? payoutIdentity);
            // ...and the worker label this connection served it under, so a share
            // arriving after a later switch is still recorded under the identity
            // the work was built for.
            this.rememberServedJobWorker(job.jobId, this.getCurrentPayoutWorker());
            this.lastSentMiningJobTimestamp = jobTemplate.block.timestamp;
            this.lastSentMiningJobSignature = signature;
            this.lastSentMiningTipKey = jobTemplate.blockData.tipKey;
            this.lastSentMiningJobType = jobTemplate.blockData.jobType;
            const bufferedAfterWrite = this.socket.writableLength ?? 0;
            if (bufferedAfterWrite >= maximumBufferedBytes) {
                this.closeSocket();
                return {
                    status: 'closed',
                    bytes: payload.length,
                    bufferedBytes: bufferedAfterWrite,
                    ...(preStagedJob == null ? {} : { preStaged: true }),
                };
            }
            return {
                status: accepted ? 'written' : 'backpressured',
                bytes: payload.length,
                bufferedBytes: bufferedAfterWrite,
                ...(preStagedJob == null ? {} : { preStaged: true }),
            };
        } catch (error) {
            this.closeSocket();
            return {
                status: 'error',
                bytes: 0,
                bufferedBytes: this.socket.writableLength ?? 0,
                ...(preStagedJob == null ? {} : { preStaged: true }),
            };
        }
    }


    private async ensureClientEntity() {
        if (this.clientEntity != null) {
            return;
        }

        if (this.creatingEntity == null) {
            this.creatingEntity = (async () => {
                this.clientEntity = await this.clientService.insert({
                    sessionId: this.extraNonceAndSessionId,
                    address: this.clientAuthorization.address,
                    clientName: this.clientAuthorization.worker,
                    userAgent: this.clientSubscription.userAgent,
                    startTime: new Date(),
                    payoutMode: this.payoutMode,
                    bestDifficulty: 0
                });
            })();
        }

        await this.creatingEntity;
    }

    private virtualWorkerKey(address: string, worker: string): string {
        return `${address}\u0000${worker}`;
    }

    // Insert-or-refresh the virtual worker presence for the current payout
    // identity. A set_payout identity has no connection of its own, so this row
    // is what keeps it on the address page's workers list. Display-only: a
    // failure is logged and retried at the next set_payout, never fatal to the
    // connection. The row gets its OWN random sessionId — sessionId carries a
    // unique index among live rows, so the connection's extranonce can't be
    // reused (accepted shares keep recording the real extranonce).
    private ensurePayoutPresence(): Promise<void> {
        const address = this.currentPayoutAddress;
        const worker = this.currentPayoutWorker ?? this.clientAuthorization.worker;
        if (this.payoutMode !== 'solo') {
            // In pplns the coinbase pays the snapshot's payees, so a set_payout
            // identity never receives work of its own — a presence row for it
            // would be a phantom worker on the address page.
            return Promise.resolve();
        }
        if (address === this.clientAuthorization.address && worker === this.clientAuthorization.worker) {
            // The authorized identity's presence is the connection's own row.
            return Promise.resolve();
        }
        const key = this.virtualWorkerKey(address, worker);
        const existing = this.virtualWorkerEntities.get(key);
        if (existing == null) {
            const inserting = this.clientService.insert({
                sessionId: this.getRandomHexString(),
                address,
                clientName: worker,
                userAgent: this.clientSubscription.userAgent,
                startTime: new Date(),
                payoutMode: this.payoutMode,
                bestDifficulty: 0
            });
            this.virtualWorkerEntities.set(key, inserting);
            return inserting.then(() => undefined, (e) => {
                // Retry with a fresh insert (and fresh random sessionId) next time.
                this.virtualWorkerEntities.delete(key);
                console.warn(`Failed to register virtual worker presence for ${address}.${worker}: ${e?.message ?? e}`);
            });
        }
        return existing.then(
            entity => this.clientService.heartbeat(entity.id).then(() => undefined),
            () => undefined
        ).catch((e) => {
            console.warn(`Failed to refresh virtual worker presence for ${address}.${worker}: ${e?.message ?? e}`);
        });
    }

    // The virtual presence row for a (payout address, worker) identity, or null
    // for the authorized identity / an identity never registered (e.g. a
    // set_payout that arrived before the stratum handshake completed).
    private async virtualWorkerEntity(address: string, worker: string): Promise<ClientEntity | null> {
        if (address === this.clientAuthorization.address && worker === this.clientAuthorization.worker) {
            return null;
        }
        const pending = this.virtualWorkerEntities.get(this.virtualWorkerKey(address, worker));
        if (pending == null) {
            return null;
        }
        return await pending.catch(() => null);
    }

    private async handleMiningSubmission(submission: MiningSubmitMessage) {

        const submissionContext = this.stratumV1JobsService.getSubmissionContext(submission.jobId);

        // a miner may submit a job that doesn't exist anymore if it was removed by a new block notification (or expired, 5 min)
        if (submissionContext == null || !this.isOwnedSubmissionContext(submissionContext)) {
            const err = new StratumErrorMessage(
                submission.id,
                eStratumErrorCode.JobNotFound,
                'Job not found').response();
            //console.log(err);
            const success = await this.write(err);
            if (!success) {
                return false;
            }
            return false;
        }

        const { job, jobTemplate, status } = submissionContext;
        // Attribute the share to the address the matched job's coinbase actually
        // pays — which can differ from clientAuthorization.address after a
        // mining.set_payout switch. Validated against the stored job above, so
        // this is exact for in-flight work. Same for the worker label: it is the
        // label this connection served THAT job under (set_payout's optional
        // second param), so a late in-flight share lands under the identity the
        // work was built for.
        const minerAddress = this.getJobMinerAddress(job);
        const minerWorker = this.getJobMinerWorker(job);

        const versionBits = this.parseUint32Hex(submission.versionMask);
        const nonce = this.parseUint32Hex(submission.nonce);
        const timestamp = this.parseUint32Hex(submission.ntime);
        if (versionBits == null || nonce == null || timestamp == null) {
            await this.writeSubmissionError(
                submission,
                eStratumErrorCode.OtherUnknown,
                'Invalid mining submit hexadecimal field',
            );
            return false;
        }
        const unsignedVersionBits = versionBits >>> 0;
        if (!Number.isInteger(versionBits)
            || versionBits < 0
            || versionBits > 0xffffffff
            || (unsignedVersionBits & (~VERSION_ROLLING_MASK >>> 0)) !== 0) {
            await this.writeSubmissionError(
                submission,
                eStratumErrorCode.OtherUnknown,
                'Invalid version mask',
            );
            return false;
        }
        // The optional BIP310 field contains replacement bits, not an XOR
        // delta. A legacy five-field submission leaves the advertised version
        // unchanged, including any bits already set inside the rolling mask.
        const effectiveVersionBits = submission.params.length >= 6
            ? unsignedVersionBits
            : (jobTemplate.block.version >>> 0) & (VERSION_ROLLING_MASK >>> 0);
        const submittedVersion = MiningJob.applyVersionRolling(
            jobTemplate.block.version,
            effectiveVersionBits,
            VERSION_ROLLING_MASK,
        );
        const requiredVersionBits = jobTemplate.blockData.requiredVersionBits ?? 0;
        if ((submittedVersion & requiredVersionBits) !== requiredVersionBits) {
            await this.writeSubmissionError(
                submission,
                eStratumErrorCode.OtherUnknown,
                'Invalid version mask',
            );
            return false;
        }

        const submissionHash = [
            job.jobId,
            submission.extraNonce2.toLowerCase(),
            timestamp.toString(16).padStart(8, '0'),
            nonce.toString(16).padStart(8, '0'),
            submittedVersion.toString(16).padStart(8, '0'),
        ].join(':');
        if (this.isDuplicateSubmission(submissionHash)) {
            const err = new StratumErrorMessage(
                submission.id,
                eStratumErrorCode.DuplicateShare,
                'Duplicate share').response();
            const success = await this.write(err);
            if (!success) {
                return false;
            }
            return false;
        }

        const header = job.buildHeaderBuffer(
            jobTemplate,
            effectiveVersionBits,
            nonce,
            this.extraNonceAndSessionId,
            submission.extraNonce2,
            timestamp,
            VERSION_ROLLING_MASK,
        );
        const { submissionDifficulty, hashBuffer } = this.calculateDifficulty(header);

        //console.log(`DIFF: ${submissionDifficulty} of ${this.sessionDifficulty} from ${this.clientAuthorization.worker + '.' + this.extraNonceAndSessionId}`);

        const isBlockCandidate = DifficultyUtils.meetsCompactTarget(
            hashBuffer,
            jobTemplate.block.bits,
        );
        if (status === 'stale' && !isBlockCandidate) {
            await this.writeSubmissionError(
                submission,
                eStratumErrorCode.JobNotFound,
                'Stale share',
            );
            return false;
        }

        const meetsSessionTarget = DifficultyUtils.meetsTarget(hashBuffer, this.sessionDifficultyTarget);
        if (status === 'current' && !isBlockCandidate && !meetsSessionTarget) {
            await this.writeSubmissionError(
                submission,
                eStratumErrorCode.LowDifficultyShare,
                'Difficulty too low',
            );
            return false;
        }
        const creditedDifficulty = isBlockCandidate && !meetsSessionTarget
            ? Math.min(this.sessionDifficulty, submissionDifficulty)
            : this.sessionDifficulty;

        let blockSubmissionResult: string = null;
        if (status === 'stale') {
            blockSubmissionResult = await this.submitBlockCandidate(
                job,
                jobTemplate,
                submission,
                effectiveVersionBits,
                nonce,
                timestamp,
            );
            if (!this.isSuccessfulBlockSubmission(blockSubmissionResult)) {
                await this.writeSubmissionError(
                    submission,
                    eStratumErrorCode.JobNotFound,
                    'Stale share',
                );
                return false;
            }
        }

        {
            const success = await this.write(JSON.stringify(submission.response()) + '\n');
            if (!success) {
                return false;
            }

            if (status === 'current' && isBlockCandidate) {
                blockSubmissionResult = await this.submitBlockCandidate(
                    job,
                    jobTemplate,
                    submission,
                    effectiveVersionBits,
                    nonce,
                    timestamp,
                );
            }
            await this.ensureClientEntity();
            // The virtual presence row for a set_payout identity (null for the
            // authorized identity): shares carry ITS clientId so the address
            // page's per-worker session summaries (rates, last-seen — keyed by
            // clientId) aggregate per payout identity, not per connection.
            const virtualEntity = await this.virtualWorkerEntity(minerAddress, minerWorker);
            try {
                // Share statistics are keyed to the job's payout address (not the
                // authorized identity) so a set_payout switch credits each address
                // with exactly the work mined for it.
                await this.shareAccountingService?.recordAcceptedShare({
                    protocol: this.accountingProtocol,
                    payoutMode: this.payoutMode,
                    address: minerAddress,
                    clientName: minerWorker,
                    sessionId: this.extraNonceAndSessionId,
                    clientId: (virtualEntity ?? this.clientEntity).id,
                    jobId: job.jobId,
                    jobTemplateId: job.jobTemplateId,
                    blockHeight: jobTemplate.blockData.height,
                    creditedDifficulty,
                    submissionDifficulty,
                    networkDifficulty: jobTemplate.blockData.networkDifficulty,
                    nonce: submission.nonce,
                    ntime: submission.ntime,
                    version: submittedVersion.toString(16),
                    extraNonce2: submission.extraNonce2,
                    isBlockCandidate,
                    blockSubmissionResult,
                });
                await this.statistics.addShares(this.clientEntity, creditedDifficulty);
                const now = new Date();
                this.clientEntity.updatedAt = now;
                this.clientEntity.hashRate = this.statistics.hashRate;
                await this.persistClientHashRate(now);

            } catch (e) {
                console.log(e);
            }

            // Best-difficulty ratchets, each gated on its own row: the connection's,
            // the payout identity's virtual row (when the share belongs to a
            // set_payout identity), and the address-level high score — gated on the
            // row matching minerAddress, NOT the connection's (a connection serving
            // many identities would otherwise stop reporting a fresh identity's best
            // once any identity had set a higher one).
            const identityBestBefore = (virtualEntity ?? this.clientEntity).bestDifficulty;
            if (submissionDifficulty > this.clientEntity.bestDifficulty) {
                await this.clientService.updateBestDifficultyIfHigher(this.clientEntity.id, submissionDifficulty);
                this.clientEntity.bestDifficulty = submissionDifficulty;
            }
            if (virtualEntity != null && submissionDifficulty > virtualEntity.bestDifficulty) {
                await this.clientService.updateBestDifficultyIfHigher(virtualEntity.id, submissionDifficulty);
                virtualEntity.bestDifficulty = submissionDifficulty;
            }
            if (submissionDifficulty > identityBestBefore) {
                await this.addressSettingsService.updateBestDifficultyIfHigher(minerAddress, submissionDifficulty, this.clientEntity.userAgent);
            }
        }

        //await this.checkDifficulty();
        return false;

    }

    private async submitBlockCandidate(
        job: MiningJob,
        jobTemplate: IJobTemplate,
        submission: MiningSubmitMessage,
        versionBits: number,
        nonce: number,
        timestamp: number,
    ): Promise<string> {
        console.log('!!! BLOCK FOUND !!!');
        const updatedJobBlock = job.copyAndUpdateBlock(
            jobTemplate,
            versionBits,
            nonce,
            this.extraNonceAndSessionId,
            submission.extraNonce2,
            timestamp,
            VERSION_ROLLING_MASK,
        );
        const minerAddress = this.getJobMinerAddress(job);
        const minerWorker = this.getJobMinerWorker(job);
        const blockHex = updatedJobBlock.toHex(false);
        const blockSubmissionResult = await this.bitcoinRpcService.SUBMIT_BLOCK(blockHex);
        await this.blocksService.save({
            height: jobTemplate.blockData.height,
            minerAddress: minerAddress,
            worker: minerWorker,
            sessionId: this.extraNonceAndSessionId,
            blockData: blockHex,
            blockSubmissionResult,
            payoutSnapshotId: this.payoutMode === 'pplns'
                ? jobTemplate.blockData.payoutSnapshotId ?? null
                : null,
            payoutMode: this.payoutMode,
        });
        if (this.payoutMode === 'pplns') {
            await this.payoutSnapshotService?.finalizeSnapshotForBlock({
                payoutSnapshotId: jobTemplate.blockData.payoutSnapshotId,
                blockHeight: jobTemplate.blockData.height,
                blockSubmissionResult,
                payoutMode: this.payoutMode,
            });
        }

        await this.notificationService.notifySubscribersBlockFound(
            minerAddress,
            jobTemplate.blockData.height,
            updatedJobBlock,
            blockSubmissionResult,
        );
        if (this.isSuccessfulBlockSubmission(blockSubmissionResult)) {
            await this.addressSettingsService.resetBestDifficultyAndShares();
        }
        return blockSubmissionResult;
    }

    private async writeSubmissionError(
        submission: MiningSubmitMessage,
        code: eStratumErrorCode,
        message: string,
    ): Promise<boolean> {
        return this.write(new StratumErrorMessage(
            submission.id,
            code,
            message,
        ).response());
    }

    private async checkDifficulty() {
        const targetDiff = this.clampDifficulty(this.statistics.getSuggestedDifficulty(this.sessionDifficulty));
        if (targetDiff == null) {
            return;
        }

        if (targetDiff != this.sessionDifficulty) {
            //console.log(`Adjusting ${this.extraNonceAndSessionId} difficulty from ${this.sessionDifficulty} to ${targetDiff}`);
            this.sessionDifficulty = targetDiff;
            this.sessionDifficultyTarget = DifficultyUtils.difficultyToTarget(this.sessionDifficulty);

            const data = JSON.stringify({
                id: null,
                method: eResponseMethod.SET_DIFFICULTY,
                params: [targetDiff]
            }) + '\n';


            await this.socket.write(data);

            const jobTemplate = await this.getLatestPayoutJobTemplate();
            const nextTimestamp = Math.max(
                jobTemplate.block.timestamp,
                Math.floor(Date.now() / 1000),
                (this.lastSentMiningJobTimestamp ?? 0) + 1
            );
            // We need to clear jobs so the difficulty takes effect, but avoid mutating or
            // re-sending the shared cached template with byte-identical work.
            const refreshedJobTemplate: IJobTemplate = {
                ...jobTemplate,
                block: Object.assign(new bitcoinjs.Block(), jobTemplate.block, {
                    timestamp: nextTimestamp
                }),
                blockData: { ...jobTemplate.blockData, clearJobs: true }
            };
            this.broadcastMiningJob(refreshedJobTemplate, true);

        }
    }

    private calculateDifficulty(header: Buffer): { submissionDifficulty: number, submissionHash: string, hashBuffer: Buffer } {

        const hashResult = hash256(header);

        const target = this.le256todouble(hashResult);
        const submissionDifficulty = target === 0 ? Number.POSITIVE_INFINITY : TRUE_DIFF_ONE / target;
        return { submissionDifficulty, submissionHash: hashResult.toString('hex'), hashBuffer: hashResult };
    }


    private le256todouble(target: Buffer): number {

        let number = 0;
        for (let i = target.length - 1; i >= 0; i--) {
            number = number * 256 + target[i];
        }

        return number;
    }

    private isBlockedUserAgent(userAgent: string): boolean {
        const blockedUserAgents = this.configService.get<string>('NON_COMPLIANT_USER_AGENTS')
            || this.configService.get<string>('BLOCKED_USER_AGENTS')
            || this.configService.get<string>('COMPLIANT_HEADERS');
        if (!blockedUserAgents || blockedUserAgents.trim() === '') {
            return false;
        }

        const blockedList = blockedUserAgents.split(',').map(ua => ua.trim().toLowerCase());
        const userAgentLower = userAgent.toLowerCase();

        return blockedList.some(blocked => blocked.length > 0 && userAgentLower.includes(blocked));
    }

    private logBlockedUserAgent(userAgent: string) {
        const now = Date.now();
        const logState = StratumV1Client.blockedUserAgentLogState.get(userAgent);

        if (logState != null && now < logState.nextLogAt) {
            logState.suppressed += 1;
            return;
        }

        const suppressed = logState?.suppressed ?? 0;
        const suffix = suppressed > 0 ? ` (${suppressed} similar connections suppressed)` : '';
        console.log(`Blocked non-compliant connection from userAgent: ${userAgent}${suffix}`);
        StratumV1Client.blockedUserAgentLogState.set(userAgent, {
            nextLogAt: now + BLOCKED_USER_AGENT_LOG_INTERVAL_MS,
            suppressed: 0
        });
    }

    private logValidationError(label: string, errors: ValidationError[]) {
        const now = Date.now();
        const signature = this.getValidationErrorSignature(errors);
        const sample = this.getValidationErrorSample(errors);
        const key = `${label}:${signature}`;
        const logState = StratumV1Client.validationErrorLogState.get(key);

        if (logState != null && now < logState.nextLogAt) {
            logState.suppressed += 1;
            return;
        }

        const suppressed = logState?.suppressed ?? 0;
        const suffix = suppressed > 0 ? ` (${suppressed} similar validation errors suppressed)` : '';
        console.warn(`${label}: ${signature}${sample}${suffix}`);
        StratumV1Client.validationErrorLogState.set(key, {
            nextLogAt: now + VALIDATION_ERROR_LOG_INTERVAL_MS,
            suppressed: 0,
            sample
        });
    }

    private async persistClientHashRate(now: Date): Promise<void> {
        if (this.clientEntity?.id == null) {
            return;
        }

        const hashRate = Number(this.statistics?.hashRate ?? 0);
        if (!Number.isFinite(hashRate) || hashRate <= 0) {
            return;
        }

        const intervalMs = this.getHashRatePersistIntervalMs();
        const nowMs = now.getTime();
        if (this.lastHashRatePersistedAt > 0 && nowMs - this.lastHashRatePersistedAt < intervalMs) {
            return;
        }

        this.lastHashRatePersistedAt = nowMs;
        try {
            await this.clientService.updateHashRate(this.clientEntity.id, hashRate, now);
        } catch (error) {
            console.error(`Failed to persist SV1 client hashrate: ${error.message}`);
        }
    }

    private getHashRatePersistIntervalMs(): number {
        const configured = Number(this.configService.get<string>('CLIENT_HASHRATE_PERSIST_INTERVAL_MS'));
        if (Number.isFinite(configured) && configured >= 0) {
            return configured;
        }

        return DEFAULT_CLIENT_HASHRATE_PERSIST_INTERVAL_MS;
    }

    private isDuplicateSubmission(submissionHash: string): boolean {
        const now = Date.now();
        const existingExpiry = this.miningSubmissionHashes.get(submissionHash);
        if (existingExpiry != null && existingExpiry > now) {
            return true;
        }
        if (existingExpiry != null) {
            this.miningSubmissionHashes.delete(submissionHash);
        }

        for (const [hash, expiresAt] of this.miningSubmissionHashes) {
            if (expiresAt <= now) {
                this.miningSubmissionHashes.delete(hash);
            }
        }

        this.miningSubmissionHashes.set(
            submissionHash,
            now + this.getSubmissionDedupTtlMs(),
        );
        const maxEntries = this.getSubmissionDedupMaxEntries();
        while (this.miningSubmissionHashes.size > maxEntries) {
            const oldestHash = this.miningSubmissionHashes.keys().next().value;
            if (oldestHash == null) {
                break;
            }
            this.miningSubmissionHashes.delete(oldestHash);
        }
        return false;
    }

    private getSubmissionDedupTtlMs(): number {
        const configured = Number(
            this.configService.get<string>('STRATUM_SUBMISSION_DEDUP_TTL_MS')
            ?? process.env.STRATUM_SUBMISSION_DEDUP_TTL_MS,
        );
        return Number.isInteger(configured) && configured > 0
            ? configured
            : DEFAULT_SUBMISSION_DEDUP_TTL_MS;
    }

    private getSubmissionDedupMaxEntries(): number {
        const configured = Number(
            this.configService.get<string>('STRATUM_SUBMISSION_DEDUP_MAX_ENTRIES')
            ?? process.env.STRATUM_SUBMISSION_DEDUP_MAX_ENTRIES,
        );
        return Number.isInteger(configured) && configured > 0
            ? configured
            : DEFAULT_SUBMISSION_DEDUP_MAX_ENTRIES;
    }

    private readMaxSocketBufferBytes(): number {
        const configured = Number(
            this.configService.get<string>('STRATUM_MAX_SOCKET_BUFFER_BYTES')
            ?? process.env.STRATUM_MAX_SOCKET_BUFFER_BYTES,
        );
        return Number.isSafeInteger(configured) && configured > 0
            ? configured
            : DEFAULT_MAX_SOCKET_BUFFER_BYTES;
    }

    private getValidationErrorSignature(errors: ValidationError[]): string {
        if (errors.length === 0) {
            return 'unknown';
        }

        return errors.map(error => {
            const constraints = Object.keys(error.constraints ?? {}).sort().join('|') || 'invalid';
            return `${error.property}:${constraints}`;
        }).join(';');
    }

    private getValidationErrorSample(errors: ValidationError[]): string {
        const values = errors
            .map(error => error.value)
            .filter(value => value != null)
            .map(value => String(value).replace(/[\r\n]/g, '').slice(0, 64));

        if (values.length === 0) {
            return '';
        }

        return ` sample=${values.join(',')}`;
    }

    private clampDifficulty(difficulty: number | null): number | null {
        if (difficulty == null || !Number.isFinite(difficulty)) {
            return null;
        }
        const configuredMinimum = this.getConfiguredMinimumDifficulty();
        return configuredMinimum == null ? difficulty : Math.max(difficulty, configuredMinimum);
    }

    private getMinimumDifficulty(): number {
        return this.getConfiguredMinimumDifficulty() ?? DEFAULT_MIN_DIFFICULTY;
    }

    private getConfiguredMinimumDifficulty(): number | null {
        const configured = parseFloat(
            this.configService.get<string>('STRATUM_MIN_DIFFICULTY')
            ?? process.env.STRATUM_MIN_DIFFICULTY
            ?? '',
        );
        return Number.isFinite(configured) && configured > 0 ? configured : null;
    }

    private getPayoutInformation(jobTemplate: IJobTemplate, fallbackAddress: string): AddressObject[] | null {
        if (this.payoutMode === 'pplns') {
            return jobTemplate.blockData.payoutOutputs?.length > 0
                ? jobTemplate.blockData.payoutOutputs
                : null;
        }

        return [{ address: fallbackAddress, percent: 100 }];
    }

    private async getLatestPayoutJobTemplate(): Promise<IJobTemplate> {
        return this.stratumV1JobsService.getLatestJobTemplate(this.payoutMode)
            ?? firstValueFrom((
                this.stratumV1JobsService.sv1MiningJob$
                ?? this.stratumV1JobsService.newMiningJob$
            ).pipe(
                filter(template => template.blockData.payoutMode === 'all'
                    || template.blockData.payoutMode === this.payoutMode),
            ));
    }

    private getCurrentPayoutAddress(): string {
        return this.currentPayoutAddress ?? this.clientAuthorization.address;
    }

    /**
     * The address a submit matched to `job` must be attributed to. It is the
     * address that job's coinbase actually pays, which can differ from the
     * connection's authorization after a mining.set_payout switch. In pplns the
     * coinbase pays the snapshot's payees rather than the submitting miner, so
     * attribution stays on the authorization there — set_payout is a solo-mode
     * concept.
     */
    private getJobMinerAddress(job: MiningJob): string {
        return (this.payoutMode === 'solo' ? job.payoutAddress : null)
            ?? this.clientAuthorization.address;
    }

    private getCurrentPayoutWorker(): string {
        return this.currentPayoutWorker ?? this.clientAuthorization?.worker;
    }

    /**
     * The worker label a submit matched to `job` must be recorded under: the
     * label THIS connection served that job under. Jobs are shared between
     * connections by payout identity, so the label cannot be read off the job
     * itself — two connections paying the same address routinely carry different
     * worker names. A job this connection was never served (or one served before
     * the LRU bound evicted it) falls back to the authorized worker, which is
     * exactly the pre-set_payout behaviour. Solo-only for the same reason as
     * getJobMinerAddress: in pplns the coinbase pays the snapshot's payees, so
     * attribution stays on the authorization.
     */
    private getJobMinerWorker(job: MiningJob): string {
        return (this.payoutMode === 'solo' ? this.servedJobWorkers.get(job.jobId) : null)
            ?? this.clientAuthorization.worker;
    }

    private rememberServedJobWorker(jobId: string, worker: string | undefined): void {
        if (jobId == null || worker == null) {
            return;
        }
        // Re-insert so the map stays in least-recently-served order.
        this.servedJobWorkers.delete(jobId);
        this.servedJobWorkers.set(jobId, worker);
        while (this.servedJobWorkers.size > SERVED_PAYOUT_IDENTITY_MAX_ENTRIES) {
            const oldest = this.servedJobWorkers.keys().next().value;
            if (oldest == null) {
                break;
            }
            this.servedJobWorkers.delete(oldest);
        }
    }

    private rememberServedPayoutIdentity(payoutIdentity: string | undefined): void {
        if (payoutIdentity == null) {
            return;
        }
        // Re-insert so the set stays in least-recently-served order.
        this.servedPayoutIdentities.delete(payoutIdentity);
        this.servedPayoutIdentities.add(payoutIdentity);
        while (this.servedPayoutIdentities.size > SERVED_PAYOUT_IDENTITY_MAX_ENTRIES) {
            const oldest = this.servedPayoutIdentities.values().next().value;
            if (oldest == null) {
                break;
            }
            this.servedPayoutIdentities.delete(oldest);
        }
    }

    private hasServedPayoutIdentity(payoutIdentity: string): boolean {
        return this.servedPayoutIdentities.has(payoutIdentity);
    }

    private getPayoutIdentity(jobTemplate: IJobTemplate): string | undefined {
        if (this.payoutMode === 'solo') {
            // Keyed on the address the coinbase pays right now, not on the
            // authorization: after mining.set_payout the connection must get (and
            // share) jobs built for the NEW address, never the cached job of the
            // address it was authorized with.
            return `solo\0${this.getCurrentPayoutAddress()}`;
        }
        if (jobTemplate.blockData.payoutSnapshotId != null) {
            return `pplns\0${jobTemplate.blockData.payoutSnapshotId}`;
        }
        return jobTemplate.blockData.payoutOutputs?.length > 0
            ? createPayoutOutputIdentity('pplns', jobTemplate.blockData.payoutOutputs)
            : undefined;
    }

    private isOwnedSubmissionContext(context: IJobSubmissionContext): boolean {
        const templatePayoutMode = context.jobTemplate.blockData.payoutMode;
        const expectedPayoutIdentity = this.getPayoutIdentity(context.jobTemplate);
        return (templatePayoutMode === 'all' || templatePayoutMode === this.payoutMode)
            && expectedPayoutIdentity != null
            && context.job.ownership?.payoutMode === this.payoutMode
            // The identity the connection is on right now, or any identity it was
            // previously served — mining.set_payout switches the coinbase mid
            // session, and a share already in flight for the previous address must
            // still validate and be attributed to that address, not dropped as
            // "Job not found". Ownership is still enforced: an identity this
            // connection was never served is rejected exactly as before.
            && (context.job.ownership.payoutIdentity === expectedPayoutIdentity
                || this.hasServedPayoutIdentity(context.job.ownership.payoutIdentity));
    }

    private parseUint32Hex(value: string | null | undefined): number | null {
        if (typeof value !== 'string' || !/^[0-9a-fA-F]{8}$/.test(value)) {
            return null;
        }
        return Number.parseInt(value, 16);
    }

    private getNetwork(): bitcoinjs.Network {
        const networkConfig = this.configService.get('NETWORK');
        if (networkConfig === 'mainnet') {
            return bitcoinjs.networks.bitcoin;
        }
        if (networkConfig === 'testnet') {
            return bitcoinjs.networks.testnet;
        }
        if (networkConfig === 'regtest') {
            return bitcoinjs.networks.regtest;
        }
        throw new Error('Invalid network configuration');
    }

    private isSuccessfulBlockSubmission(result?: string | null): boolean {
        return result == null || result === 'SUCCESS!';
    }

    private closeSocket() {
        this.connectionClosed = true;
        if (!this.socket.destroyed) {
            this.socket.destroy();
        }
    }

    private async write(message: string): Promise<boolean> {
        try {
            if (!this.socket.destroyed && !this.socket.writableEnded) {

                await new Promise((resolve, reject) => {
                    this.socket.write(message, (error) => {
                        if (error) {
                            reject(error);
                        } else {
                            resolve(true);
                        }
                    });
                });

                return true;
            } else {
                //console.error(`Error: Cannot write to closed or ended socket. ${this.extraNonceAndSessionId} ${message}`);
                this.destroy();
                if (!this.socket.destroyed) {
                    this.socket.destroy();
                }
                return false;
            }
        } catch (error) {
            this.destroy();
            if (!this.socket.writableEnded) {
                await this.socket.end();
            } else if (!this.socket.destroyed) {
                this.socket.destroy();
            }
            //console.error(`Error occurred while writing to socket: ${this.extraNonceAndSessionId}`, error);
            return false;
        }
    }

}
