import { ConfigService } from '@nestjs/config';
import * as bitcoinjs from 'bitcoinjs-lib';
import { Socket } from 'net';
import { BehaviorSubject } from 'rxjs';

import { MockRecording1 } from '../../test/models/MockRecording1';
import { AddressSettingsService } from '../ORM/address-settings/address-settings.service';
import { BlocksService } from '../ORM/blocks/blocks.service';
import { ClientService } from '../ORM/client/client.service';
import { BitcoinRpcService as MockBitcoinRpcService } from '../services/bitcoin-rpc.service';
import { NotificationService } from '../services/notification.service';
import { StratumV1JobsService } from '../services/stratum-v1-jobs.service';
import { DifficultyUtils } from '../utils/difficulty.utils';
import { IBlockTemplate } from './bitcoin-rpc/IBlockTemplate';
import { MiningJob } from './MiningJob';
import { StratumV1Client } from './StratumV1Client';
import { MiningSubmitMessage } from './stratum-messages/MiningSubmitMessage';





jest.mock('../services/bitcoin-rpc.service')

jest.mock('./validators/bitcoin-address.validator', () => ({
    IsBitcoinAddress() {
        return jest.fn();
    },
}));


describe('StratumV1Client', () => {


    let socket: Socket;
    let stratumV1JobsService: StratumV1JobsService;
    let bitcoinRpcService: MockBitcoinRpcService;

    let clientService: ClientService;
    let notificationService: NotificationService;
    let blocksService: BlocksService;
    let configService: ConfigService;
    let addressSettings: AddressSettingsService;
    let shareAccountingService: { recordAcceptedShare: jest.Mock };
    let redisMessagingService: Record<string, never>;

    let client: StratumV1Client;

    let socketEmitter: (...args: any[]) => void;
    const emitMessage = (message: string) => socketEmitter(Buffer.from(`${message}\n`));
    let consoleLogSpy: jest.SpyInstance;
    let consoleErrorSpy: jest.SpyInstance;
    let consoleWarnSpy: jest.SpyInstance;

    let newBlockEmitter: BehaviorSubject<IBlockTemplate> = new BehaviorSubject(MockRecording1.BLOCK_TEMPLATE);

    beforeEach(async () => {

        jest.useFakeTimers({ advanceTimers: true })
        jest.setSystemTime(new Date(parseInt(MockRecording1.TIME, 16) * 1000));
        consoleLogSpy = jest.spyOn(console, 'log').mockImplementation(() => undefined);
        consoleErrorSpy = jest.spyOn(console, 'error').mockImplementation(() => undefined);
        consoleWarnSpy = jest.spyOn(console, 'warn').mockImplementation(() => undefined);

        const clients = new Map<string, any>();
        let nextClientId = 1;
        clientService = {
            insert: jest.fn(async (partialClient) => {
                const clientEntity = {
                    id: `00000000-0000-4000-8000-${String(nextClientId++).padStart(12, '0')}`,
                    hashRate: 0,
                    updatedAt: new Date(),
                    deletedAt: null,
                    ...partialClient,
                };
                clients.set(clientEntity.id, clientEntity);
                return clientEntity;
            }),
            delete: jest.fn(async (id: string) => {
                clients.delete(id);
            }),
            connectedClientCount: jest.fn(async () => clients.size),
            updateBestDifficultyIfHigher: jest.fn().mockResolvedValue({ affected: 1 }),
            updateHashRate: jest.fn().mockResolvedValue(undefined),
            heartbeat: jest.fn().mockResolvedValue(undefined),
        } as any;

        configService = {
            get: jest.fn((key: string) => {
                switch (key) {
                    case 'DEV_FEE_ADDRESS':
                        return 'tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4';
                    case 'NETWORK':
                        return 'testnet';
                }
                return null;
            })
        } as any;
        (StratumV1Client as any).blockedUserAgentLogState.clear();
        (StratumV1Client as any).validationErrorLogState.clear();

        bitcoinRpcService = {
            newBlockTemplate$: newBlockEmitter.asObservable(),
            miningInfo: { blocks: MockRecording1.BLOCK_TEMPLATE.height },
            SUBMIT_BLOCK: jest.fn().mockResolvedValue(null)
        } as any;


        stratumV1JobsService = new StratumV1JobsService(bitcoinRpcService);

        socket = new Socket();
        // jest.spyOn(socket, 'on').mockImplementation((event: string, fn: (data: Buffer) => void) => {
        //     socketEmitter = fn;
        // });

        jest.spyOn(socket, 'on').mockImplementation((event: string, listener: (...args: any[]) => void) => {
            socketEmitter = listener;
            return socket;
        });

        socket.end = jest.fn();
        jest.spyOn(socket, 'destroy').mockImplementation(() => socket);

        addressSettings = {
            getSettings: jest.fn().mockResolvedValue(null),
            updateBestDifficultyIfHigher: jest.fn().mockResolvedValue({ affected: 1 }),
            resetBestDifficultyAndShares: jest.fn().mockResolvedValue(undefined),
        } as any;
        notificationService = {
            notifySubscribersBlockFound: jest.fn().mockResolvedValue(undefined)
        } as any;
        blocksService = {
            save: jest.fn().mockResolvedValue(undefined)
        } as any;
        shareAccountingService = {
            recordAcceptedShare: jest.fn().mockResolvedValue(undefined),
        };
        redisMessagingService = {};


        client = new StratumV1Client(
            socket,
            stratumV1JobsService,
            bitcoinRpcService,
            clientService,
            notificationService,
            blocksService,
            configService,
            addressSettings,
            shareAccountingService as any,
            redisMessagingService as any
        );

        client.extraNonceAndSessionId = MockRecording1.EXTRA_NONCE;
        jest.spyOn(client as any, 'getRandomHexString').mockReturnValue(MockRecording1.EXTRA_NONCE);

    });

    afterEach(async () => {
        client.destroy();
        consoleLogSpy.mockRestore();
        consoleErrorSpy.mockRestore();
        consoleWarnSpy.mockRestore();
        jest.useRealTimers();
    })


    it('should subscribe to socket', () => {
        expect(socket.on).toHaveBeenCalled();
    });

    it('should clean up socket state only once when destroyed repeatedly', async () => {
        const unsubscribe = jest.fn();
        const timer = setInterval(() => undefined, 1000);
        const removeListenerSpy = jest.spyOn(socket, 'removeListener');

        (client as any).clientEntity = {
            id: '00000000-0000-4000-8000-000000000001',
            address: 'tb1qcleanup',
        };
        (client as any).stratumSubscription = { unsubscribe };
        (client as any).backgroundWork = [timer];
        (client as any).miningSubmissionHashes.add('submitted-share');
        (client as any).buffer = 'partial-message';

        await Promise.all([client.destroy(), client.destroy()]);

        expect(clientService.delete).toHaveBeenCalledTimes(1);
        expect(clientService.delete).toHaveBeenCalledWith('00000000-0000-4000-8000-000000000001');
        expect(unsubscribe).toHaveBeenCalledTimes(1);
        expect(removeListenerSpy).toHaveBeenCalledWith('data', expect.any(Function));
        expect((client as any).backgroundWork).toEqual([]);
        expect((client as any).miningSubmissionHashes.size).toBe(0);
        expect((client as any).buffer).toBe('');
    });

    it('should close socket on invalid JSON', () => {
        emitMessage('INVALID');
        jest.spyOn(socket, 'destroy');
        expect(socket.on).toHaveBeenCalled();
    });

    it('should respond to mining.subscribe', async () => {
        jest.spyOn(socket, 'write').mockImplementation((data) => true);

        expect(socket.on).toHaveBeenCalled();
        emitMessage(MockRecording1.MINING_SUBSCRIBE);

        await new Promise((r) => setTimeout(r, 1));

        expect(socket.write).toHaveBeenCalledWith(`{"id":1,"error":null,"result":[[["mining.notify","${client.extraNonceAndSessionId}"]],"${client.extraNonceAndSessionId}",8]}\n`, expect.any(Function));

    });

    it('should disable application idle timeout after Stratum initialization', async () => {
        const setTimeoutSpy = jest.spyOn(socket, 'setTimeout').mockImplementation(() => socket);
        jest.spyOn(client as any, 'write').mockImplementation(() => Promise.resolve(true));

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        expect(setTimeoutSpy).toHaveBeenCalledWith(0);
    });

    it('should block non-compliant user agents on subscribe without allocating a session', async () => {
        (configService.get as jest.Mock).mockImplementation((key: string) => {
            switch (key) {
                case 'NON_COMPLIANT_USER_AGENTS':
                    return 'NMMiner';
                case 'DEV_FEE_ADDRESS':
                    return 'tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4';
                case 'NETWORK':
                    return 'testnet';
            }
            return null;
        });
        jest.spyOn(socket, 'write').mockImplementation((data) => true);

        emitMessage(`{"id":1,"method":"mining.subscribe","params":["NMMiner/1.0"]}`);
        await new Promise((r) => setTimeout(r, 1));

        expect(socket.destroy).toHaveBeenCalled();
        expect(socket.write).not.toHaveBeenCalled();
        expect((client as any).statistics).toBeUndefined();
        expect(consoleLogSpy).toHaveBeenCalledWith('Blocked non-compliant connection from userAgent: NMMiner');
    });

    it('should throttle repeated non-compliant user agent logs', async () => {
        (configService.get as jest.Mock).mockImplementation((key: string) => {
            switch (key) {
                case 'NON_COMPLIANT_USER_AGENTS':
                    return 'NMMiner';
                case 'DEV_FEE_ADDRESS':
                    return 'tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4';
                case 'NETWORK':
                    return 'testnet';
            }
            return null;
        });
        jest.spyOn(socket, 'write').mockImplementation((data) => true);

        emitMessage(`{"id":1,"method":"mining.subscribe","params":["NMMiner/1.0"]}`);
        await new Promise((r) => setTimeout(r, 1));

        const secondSocket = new Socket();
        jest.spyOn(secondSocket, 'on').mockImplementation((event: string, listener: (...args: any[]) => void) => {
            socketEmitter = listener;
            return secondSocket;
        });
        secondSocket.end = jest.fn();
        jest.spyOn(secondSocket, 'destroy').mockImplementation(() => secondSocket);
        const secondClient = new StratumV1Client(
            secondSocket,
            stratumV1JobsService,
            bitcoinRpcService,
            clientService,
            notificationService,
            blocksService,
            configService,
            addressSettings
        );

        socketEmitter(Buffer.from(`{"id":1,"method":"mining.subscribe","params":["NMMiner/1.0"]}\n`));
        await new Promise((r) => setTimeout(r, 1));

        expect(secondSocket.destroy).toHaveBeenCalled();
        expect(consoleLogSpy.mock.calls.filter(call => call[0]?.startsWith('Blocked non-compliant connection'))).toHaveLength(1);
        await secondClient.destroy();
    });


    it('should respond to mining.configure', async () => {

        jest.spyOn(socket, 'write').mockImplementation((data) => true);

        expect(socket.on).toHaveBeenCalled();
        emitMessage(MockRecording1.MINING_CONFIGURE);
        await new Promise((r) => setTimeout(r, 1));
        expect(socket.write).toHaveBeenCalledWith(`{"id":2,"error":null,"result":{"version-rolling":true,"version-rolling.mask":"1fffe000"}}\n`, expect.any(Function));
    });

    it('should respond to mining.authorize', async () => {

        jest.spyOn(socket, 'write').mockImplementation((data) => true);

        expect(socket.on).toHaveBeenCalled();
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 1));
        expect(socket.write).toHaveBeenCalledWith('{"id":3,"error":null,"result":true}\n', expect.any(Function));
    });

    it('should respond to mining.suggest_difficulty', async () => {
        jest.spyOn(socket, 'write').mockImplementation((data) => true);

        expect(socket.on).toHaveBeenCalled();
        emitMessage(MockRecording1.MINING_SUGGEST_DIFFICULTY);
        await new Promise((r) => setTimeout(r, 1));
        expect(socket.write).toHaveBeenCalledWith(`{"id":null,"method":"mining.set_difficulty","params":[512]}\n`, expect.any(Function));
    });

    it('should clamp suggested difficulty to the configured minimum', async () => {
        (configService.get as jest.Mock).mockImplementation((key: string) => {
            switch (key) {
                case 'STRATUM_MIN_DIFFICULTY':
                    return '1';
                case 'DEV_FEE_ADDRESS':
                    return 'tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4';
                case 'NETWORK':
                    return 'testnet';
            }
            return null;
        });
        jest.spyOn(socket, 'write').mockImplementation((data) => true);

        emitMessage(`{"id":4,"method":"mining.suggest_difficulty","params":[0]}`);
        await new Promise((r) => setTimeout(r, 1));

        expect(socket.write).toHaveBeenCalledWith(`{"id":null,"method":"mining.set_difficulty","params":[1]}\n`, expect.any(Function));
    });

    it('should set difficulty', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        expect((client as any).write).toHaveBeenCalledWith(`{"id":null,"method":"mining.set_difficulty","params":[100000]}\n`);

    });

    it('should save client', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));
        emitMessage(MockRecording1.MINING_SUBMIT);
        await new Promise((r) => setTimeout(r, 100));

        const clientCount = await clientService.connectedClientCount();
        expect(clientCount).toBe(1);

    });




    it('should send job and accept submission', async () => {



        const date = new Date(parseInt(MockRecording1.TIME, 16) * 1000);


        jest.setSystemTime(date);

        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));


        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);



        await new Promise((r) => setTimeout(r, 100));




        // coinbase input script carries the pool.powermining.io tag (cc778c3)
        expect((client as any).write).lastCalledWith(`{"id":null,"method":"mining.notify","params":["1","171592f223740e92d223f6e68bff25279af7ac4f2246451e0000000200000000","02000000010000000000000000000000000000000000000000000000000000000000000000ffffffff2303c94325706f6f6c2e706f7765726d696e696e672e696f","ffffffff02b59f250000000000160014e6f22ca44dc800e9d049621a3b9a42c509f1c4bc0000000000000000266a24aa21a9edbd3d1d916aa0b57326a2d88ebe1b68a1d7c48585f26d8335fe6a94b62755f64c00000000",["175335649d5e8746982969ec88f52e85ac9917106fba5468e699c8879ab974a1","d5644ab3e708c54cd68dc5aedc92b8d3037449687f92ec41ed6e37673d969d4a","5c9ec187517edc0698556cca5ce27e54c96acb014770599ed9df4d4937fbf2b0"],"20000000","192495f8","${MockRecording1.TIME}",true]}\n`);


        emitMessage(MockRecording1.MINING_SUBMIT);

        jest.useRealTimers();
        await new Promise((r) => setTimeout(r, 1000));

        expect((client as any).write).lastCalledWith(`{\"id\":5,\"error\":null,\"result\":true}\n`);
        expect(shareAccountingService.recordAcceptedShare).toHaveBeenCalledWith(expect.objectContaining({
            protocol: 'sv1',
            address: 'tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4',
            clientName: 'bitaxe3',
            sessionId: MockRecording1.EXTRA_NONCE,
            jobId: '1',
            jobTemplateId: '1',
            creditedDifficulty: 0,
            isBlockCandidate: false,
        }));
    });

    it('should use the header-only fast path for non-block submissions', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));
        const buildHeaderSpy = jest.spyOn(MiningJob.prototype, 'buildHeaderBuffer');
        const fullBlockSpy = jest.spyOn(MiningJob.prototype, 'copyAndUpdateBlock');

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        emitMessage(MockRecording1.MINING_SUBMIT);
        jest.useRealTimers();
        await new Promise((r) => setTimeout(r, 1000));

        expect(buildHeaderSpy).toHaveBeenCalled();
        expect(fullBlockSpy).not.toHaveBeenCalled();
    });

    it('should write accepted response before share accounting finishes', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        let finishAccounting: () => void;
        const accountingPromise = new Promise<void>((resolve) => {
            finishAccounting = resolve;
        });
        jest.spyOn((client as any).statistics, 'addShares').mockReturnValue(accountingPromise);

        emitMessage(MockRecording1.MINING_SUBMIT);
        await Promise.resolve();
        await Promise.resolve();
        await Promise.resolve();

        expect((client as any).write).toHaveBeenCalledWith(`{"id":5,"error":null,"result":true}\n`);

        finishAccounting();
        jest.useRealTimers();
        await new Promise((r) => setTimeout(r, 100));
    });

    it('should update address best difficulty through the atomic path', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));
        jest.spyOn(client as any, 'calculateDifficulty').mockReturnValue({
            submissionDifficulty: 1024,
            submissionHash: 'share',
            hashBuffer: DifficultyUtils.difficultyToTarget(1024),
        });
        const getSettingsSpy = jest.spyOn(addressSettings, 'getSettings');
        const updateIfHigherSpy = jest.spyOn(addressSettings as any, 'updateBestDifficultyIfHigher').mockResolvedValue({ affected: 1 });
        const clientUpdateIfHigherSpy = jest.spyOn(clientService as any, 'updateBestDifficultyIfHigher').mockResolvedValue({ affected: 1 });

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        emitMessage(MockRecording1.MINING_SUBMIT);
        jest.useRealTimers();
        await new Promise((r) => setTimeout(r, 1000));

        expect(updateIfHigherSpy).toHaveBeenCalledWith(
            'tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4',
            1024,
            expect.any(String)
        );
        expect(clientUpdateIfHigherSpy).toHaveBeenCalledWith(expect.any(String), 1024);
        expect(getSettingsSpy).not.toHaveBeenCalled();
    });

    it('should reject shares by exact target even when reported difficulty is huge', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));
        jest.spyOn(client as any, 'calculateDifficulty').mockReturnValue({
            submissionDifficulty: Number.MAX_SAFE_INTEGER,
            submissionHash: 'too-easy',
            hashBuffer: Buffer.alloc(32, 0xff),
        });

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [1024]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        emitMessage(MockRecording1.MINING_SUBMIT);
        jest.useRealTimers();
        await new Promise((r) => setTimeout(r, 1000));

        expect((client as any).write).lastCalledWith(`{"id":5,"result":null,"error":[23,"Difficulty too low",""]}\n`);
        expect(shareAccountingService.recordAcceptedShare).not.toHaveBeenCalled();
    });

    it('should reject duplicate submissions', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        emitMessage(MockRecording1.MINING_SUBMIT);
        await new Promise((r) => setTimeout(r, 100));
        emitMessage(MockRecording1.MINING_SUBMIT);
        await new Promise((r) => setTimeout(r, 100));

        expect((client as any).write).lastCalledWith(`{"id":5,"result":null,"error":[22,"Duplicate share",""]}\n`);
    });

    it('should reject submissions for unknown jobs', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));
        const hashSpy = jest.spyOn(MiningSubmitMessage.prototype, 'hash');

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        emitMessage(`{"id": 5, "method": "mining.submit", "params": ["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4.bitaxe3", "ff", "c708000000000000", "64b3f3ec", "ed460d91", "00002000"]}`);
        await new Promise((r) => setTimeout(r, 100));

        expect((client as any).write).lastCalledWith(`{"id":5,"result":null,"error":[21,"Job not found",""]}\n`);
        expect(await clientService.connectedClientCount()).toBe(1);
        expect(hashSpy).not.toHaveBeenCalled();
        expect(shareAccountingService.recordAcceptedShare).not.toHaveBeenCalled();
    });

    it('should reject submissions when the job template has expired', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));
        stratumV1JobsService.blocks = {};

        emitMessage(MockRecording1.MINING_SUBMIT);
        await new Promise((r) => setTimeout(r, 100));

        expect((client as any).write).lastCalledWith(`{"id":5,"result":null,"error":[21,"Job Template not found",""]}\n`);
    });

    it('should reject low difficulty shares', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(MockRecording1.MINING_SUGGEST_DIFFICULTY);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        emitMessage(MockRecording1.MINING_SUBMIT);
        await new Promise((r) => setTimeout(r, 100));

        expect((client as any).write).lastCalledWith(`{"id":5,"result":null,"error":[23,"Difficulty too low",""]}\n`);
        expect(await clientService.connectedClientCount()).toBe(1);
        expect(shareAccountingService.recordAcceptedShare).not.toHaveBeenCalled();
    });

    it('should reject submissions with short extranonce2 values', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        emitMessage(`{"id": 5, "method": "mining.submit", "params": ["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4.bitaxe3", "1", "c7080000", "64b3f3ec", "ed460d91", "00002000"]}`);
        await new Promise((r) => setTimeout(r, 100));

        expect((client as any).write).lastCalledWith(expect.stringContaining(`"error":[20,"Mining Submit validation error"`));
        expect(socket.destroy).toHaveBeenCalled();
        expect(consoleWarnSpy).toHaveBeenCalledWith(expect.stringContaining('Mining Submit validation error: extraNonce2:isLength'));
    });

    it('should throttle repeated mining submit validation logs', async () => {
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        emitMessage(`{"id": 5, "method": "mining.submit", "params": ["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4.bitaxe3", "1", "c7080000", "64b3f3ec", "ed460d91", "00002000"]}`);
        await new Promise((r) => setTimeout(r, 100));

        const secondSocket = new Socket();
        jest.spyOn(secondSocket, 'on').mockImplementation((event: string, listener: (...args: any[]) => void) => {
            socketEmitter = listener;
            return secondSocket;
        });
        secondSocket.end = jest.fn();
        jest.spyOn(secondSocket, 'destroy').mockImplementation(() => secondSocket);
        const secondClient = new StratumV1Client(
            secondSocket,
            stratumV1JobsService,
            bitcoinRpcService,
            clientService,
            notificationService,
            blocksService,
            configService,
            addressSettings
        );
        jest.spyOn(secondClient as any, 'write').mockImplementation((data) => Promise.resolve(true));
        jest.spyOn(secondClient as any, 'getRandomHexString').mockReturnValue(MockRecording1.EXTRA_NONCE);

        socketEmitter(Buffer.from(`${MockRecording1.MINING_SUBSCRIBE}\n`));
        socketEmitter(Buffer.from(`${MockRecording1.MINING_AUTHORIZE}\n`));
        await new Promise((r) => setTimeout(r, 100));
        socketEmitter(Buffer.from(`{"id": 5, "method": "mining.submit", "params": ["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4.bitaxe3", "1", "c7080000", "64b3f3ec", "ed460d91", "00002000"]}\n`));
        await new Promise((r) => setTimeout(r, 100));

        expect(consoleWarnSpy.mock.calls.filter(call => call[0]?.startsWith('Mining Submit validation error'))).toHaveLength(1);
        await secondClient.destroy();
    });

    it('should close socket when a submit arrives before stratum is initialized', async () => {
        const endSpy = jest.spyOn(socket, 'end');

        emitMessage(MockRecording1.MINING_SUBMIT);
        await new Promise((r) => setTimeout(r, 100));

        expect(endSpy).toHaveBeenCalled();
    });

    it('should submit and persist found blocks', async () => {
        (bitcoinRpcService.SUBMIT_BLOCK as jest.Mock).mockResolvedValue('SUCCESS!');
        jest.spyOn(client as any, 'write').mockImplementation((data) => Promise.resolve(true));
        jest.spyOn(client as any, 'calculateDifficulty').mockReturnValue({
            submissionDifficulty: Number.MAX_SAFE_INTEGER,
            submissionHash: 'block-share',
            hashBuffer: Buffer.alloc(32),
        });
        jest.spyOn(addressSettings, 'resetBestDifficultyAndShares').mockResolvedValue(undefined);

        emitMessage(MockRecording1.MINING_SUBSCRIBE);
        emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
        emitMessage(MockRecording1.MINING_AUTHORIZE);
        await new Promise((r) => setTimeout(r, 100));

        emitMessage(MockRecording1.MINING_SUBMIT);
        jest.useRealTimers();
        await new Promise((r) => setTimeout(r, 1000));

        expect(bitcoinRpcService.SUBMIT_BLOCK).toHaveBeenCalledWith(expect.any(String));
        expect(blocksService.save).toHaveBeenCalledWith(expect.objectContaining({
            height: MockRecording1.BLOCK_TEMPLATE.height,
            minerAddress: 'tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4',
            worker: 'bitaxe3',
            sessionId: MockRecording1.EXTRA_NONCE,
            blockData: expect.any(String),
            payoutSnapshotId: null,
            payoutMode: 'solo',
        }));
        expect(notificationService.notifySubscribersBlockFound).toHaveBeenCalled();
        expect(addressSettings.resetBestDifficultyAndShares).toHaveBeenCalled();
        expect((client as any).write).lastCalledWith(`{"id":5,"error":null,"result":true}\n`);
    });


    describe('mining.set_payout', () => {

        const AUTHORIZED_ADDRESS = 'tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4';
        const AUTHORIZED_SCRIPT = bitcoinjs.payments.p2wpkh({ address: AUTHORIZED_ADDRESS, network: bitcoinjs.networks.testnet }).output.toString('hex');
        const NEW_PAYOUT = bitcoinjs.payments.p2wpkh({ hash: Buffer.alloc(20, 0xab), network: bitcoinjs.networks.testnet });
        const NEW_PAYOUT_ADDRESS = NEW_PAYOUT.address;
        const NEW_PAYOUT_SCRIPT = NEW_PAYOUT.output.toString('hex');

        const lastNotifyParams = (written: string[]) => {
            const notifies = written.filter(m => m.includes('"mining.notify"'));
            expect(notifies.length).toBeGreaterThan(0);
            return JSON.parse(notifies[notifies.length - 1]).params;
        };

        it('should switch the coinbase payout address and push a clean job on the same extranonce', async () => {
            const written: string[] = [];
            jest.spyOn(client as any, 'write').mockImplementation((data: string) => { written.push(data); return Promise.resolve(true); });

            emitMessage(MockRecording1.MINING_SUBSCRIBE);
            emitMessage(MockRecording1.MINING_AUTHORIZE);
            await new Promise((r) => setTimeout(r, 100));

            // the job served after authorize pays the authorized address
            let coinbase = lastNotifyParams(written)[2] + lastNotifyParams(written)[3];
            expect(coinbase).toContain(AUTHORIZED_SCRIPT);

            const extraNonceBefore = client.extraNonceAndSessionId;
            const writesBefore = written.length;

            emitMessage(`{"id": 7, "method": "mining.set_payout", "params": ["${NEW_PAYOUT_ADDRESS}"]}`);
            await new Promise((r) => setTimeout(r, 100));

            // acked, and a fresh clean job pays the new address — with no re-subscribe
            // and no extranonce change
            expect(written.slice(writesBefore)).toContain('{"id":7,"error":null,"result":true}\n');
            const notify = lastNotifyParams(written);
            coinbase = notify[2] + notify[3];
            expect(coinbase).toContain(NEW_PAYOUT_SCRIPT);
            expect(coinbase).not.toContain(AUTHORIZED_SCRIPT);
            expect(notify[8]).toBe(true); // clean_jobs
            expect(client.extraNonceAndSessionId).toBe(extraNonceBefore);
            expect(written.slice(writesBefore).some(m => m.includes('mining.set_extranonce'))).toBe(false);
        });

        it('should answer a set_payout without params with an error, not a dropped connection', async () => {
            const written: string[] = [];
            jest.spyOn(client as any, 'write').mockImplementation((data: string) => { written.push(data); return Promise.resolve(true); });

            emitMessage(MockRecording1.MINING_SUBSCRIBE);
            emitMessage(MockRecording1.MINING_AUTHORIZE);
            await new Promise((r) => setTimeout(r, 100));

            emitMessage(`{"id": 9, "method": "mining.set_payout"}`);
            await new Promise((r) => setTimeout(r, 100));

            expect(socket.end).not.toHaveBeenCalled();
            const response = written.find(m => m.includes('"id":9'));
            expect(response).toBeDefined();
            expect(response).toContain('"result":null');
        });

        it('should attribute shares to the job\'s payout address across a switch', async () => {
            const written: string[] = [];
            jest.spyOn(client as any, 'write').mockImplementation((data: string) => { written.push(data); return Promise.resolve(true); });

            emitMessage(MockRecording1.MINING_SUBSCRIBE);
            emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
            emitMessage(MockRecording1.MINING_AUTHORIZE);
            await new Promise((r) => setTimeout(r, 100));

            const jobBefore = lastNotifyParams(written)[0];
            expect(jobBefore).toEqual('1'); // the recorded MINING_SUBMIT references job 1

            emitMessage(`{"id": 7, "method": "mining.set_payout", "params": ["${NEW_PAYOUT_ADDRESS}"]}`);
            await new Promise((r) => setTimeout(r, 100));

            const jobAfter = lastNotifyParams(written)[0];
            expect(jobAfter).not.toEqual(jobBefore);

            jest.useRealTimers();

            // in-flight share computed against the pre-switch job: still accepted,
            // attributed to the OLD address
            emitMessage(MockRecording1.MINING_SUBMIT);
            await new Promise((r) => setTimeout(r, 1000));
            expect(written).toContain('{"id":5,"error":null,"result":true}\n');

            // share on the post-switch job: attributed to the NEW address
            // (session difficulty was suggested to 0, so any nonce clears it)
            emitMessage(`{"id": 8, "method": "mining.submit", "params": ["${AUTHORIZED_ADDRESS}.bitaxe3", "${jobAfter}", "c708000000000001", "${MockRecording1.TIME}", "ed460d91", "00002000"]}`);
            await new Promise((r) => setTimeout(r, 1000));
            expect(written).toContain('{"id":8,"error":null,"result":true}\n');

            const recorded = shareAccountingService.recordAcceptedShare.mock.calls.map(call => call[0]);
            expect(recorded.length).toBe(2);
            expect(recorded.filter(r => r.address === AUTHORIZED_ADDRESS && r.jobId === jobBefore).length).toBe(1);
            expect(recorded.filter(r => r.address === NEW_PAYOUT_ADDRESS && r.jobId === jobAfter).length).toBe(1);
        });

        it('should record shares under the job\'s worker param across a switch', async () => {
            const written: string[] = [];
            jest.spyOn(client as any, 'write').mockImplementation((data: string) => { written.push(data); return Promise.resolve(true); });

            emitMessage(MockRecording1.MINING_SUBSCRIBE);
            emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
            emitMessage(MockRecording1.MINING_AUTHORIZE);
            await new Promise((r) => setTimeout(r, 100));

            const jobBefore = lastNotifyParams(written)[0];

            emitMessage(`{"id": 7, "method": "mining.set_payout", "params": ["${NEW_PAYOUT_ADDRESS}", "o32"]}`);
            await new Promise((r) => setTimeout(r, 100));

            const jobAfter = lastNotifyParams(written)[0];
            expect(jobAfter).not.toEqual(jobBefore);

            jest.useRealTimers();

            // in-flight share on the pre-switch job: recorded under the worker the
            // job was built for (the authorized worker), not the current identity
            emitMessage(MockRecording1.MINING_SUBMIT);
            await new Promise((r) => setTimeout(r, 1000));

            // share on the post-switch job: recorded under the set_payout worker
            emitMessage(`{"id": 8, "method": "mining.submit", "params": ["${AUTHORIZED_ADDRESS}.bitaxe3", "${jobAfter}", "c708000000000001", "${MockRecording1.TIME}", "ed460d91", "00002000"]}`);
            await new Promise((r) => setTimeout(r, 1000));

            const recorded = shareAccountingService.recordAcceptedShare.mock.calls.map(call => call[0]);
            expect(recorded.length).toBe(2);
            expect(recorded.filter(r => r.jobId === jobBefore && r.clientName === 'bitaxe3' && r.address === AUTHORIZED_ADDRESS).length).toBe(1);
            expect(recorded.filter(r => r.jobId === jobAfter && r.clientName === 'o32' && r.address === NEW_PAYOUT_ADDRESS).length).toBe(1);
        });

        it('should fall back to the authorized worker when set_payout omits the worker param', async () => {
            const written: string[] = [];
            jest.spyOn(client as any, 'write').mockImplementation((data: string) => { written.push(data); return Promise.resolve(true); });

            emitMessage(MockRecording1.MINING_SUBSCRIBE);
            emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
            emitMessage(MockRecording1.MINING_AUTHORIZE);
            await new Promise((r) => setTimeout(r, 100));

            emitMessage(`{"id": 7, "method": "mining.set_payout", "params": ["${NEW_PAYOUT_ADDRESS}"]}`);
            await new Promise((r) => setTimeout(r, 100));
            const jobAfter = lastNotifyParams(written)[0];

            jest.useRealTimers();
            emitMessage(`{"id": 8, "method": "mining.submit", "params": ["${AUTHORIZED_ADDRESS}.bitaxe3", "${jobAfter}", "c708000000000001", "${MockRecording1.TIME}", "ed460d91", "00002000"]}`);
            await new Promise((r) => setTimeout(r, 1000));

            const recorded = shareAccountingService.recordAcceptedShare.mock.calls.map(call => call[0]);
            expect(recorded.length).toBe(1);
            expect(recorded[0].address).toBe(NEW_PAYOUT_ADDRESS);
            expect(recorded[0].clientName).toBe('bitaxe3');
        });

        it('should maintain a virtual worker presence per (address, worker)', async () => {
            jest.spyOn(client as any, 'write').mockImplementation(() => Promise.resolve(true));

            emitMessage(MockRecording1.MINING_SUBSCRIBE);
            emitMessage(MockRecording1.MINING_AUTHORIZE);
            await new Promise((r) => setTimeout(r, 100));

            const insertMock = clientService.insert as jest.Mock;
            const insertsAfterAuthorize = insertMock.mock.calls.length; // the connection's own row

            emitMessage(`{"id": 7, "method": "mining.set_payout", "params": ["${NEW_PAYOUT_ADDRESS}", "o32"]}`);
            await new Promise((r) => setTimeout(r, 100));

            // one virtual row inserted for the new identity
            expect(insertMock.mock.calls.length).toBe(insertsAfterAuthorize + 1);
            expect(insertMock).toHaveBeenLastCalledWith(expect.objectContaining({
                address: NEW_PAYOUT_ADDRESS,
                clientName: 'o32',
                sessionId: expect.any(String),
                bestDifficulty: 0,
            }));
            const virtualEntity = await insertMock.mock.results[insertMock.mock.results.length - 1].value;

            // a repeat switch to the same identity refreshes the row, no new insert
            emitMessage(`{"id": 9, "method": "mining.set_payout", "params": ["${NEW_PAYOUT_ADDRESS}", "o32"]}`);
            await new Promise((r) => setTimeout(r, 100));
            expect(insertMock.mock.calls.length).toBe(insertsAfterAuthorize + 1);
            expect((clientService as any).heartbeat).toHaveBeenCalledWith(virtualEntity.id);

            // switching back to the authorized identity needs no virtual row
            emitMessage(`{"id": 10, "method": "mining.set_payout", "params": ["${AUTHORIZED_ADDRESS}", "bitaxe3"]}`);
            await new Promise((r) => setTimeout(r, 100));
            expect(insertMock.mock.calls.length).toBe(insertsAfterAuthorize + 1);

            // the virtual presence dies with the connection
            await client.destroy();
            expect(clientService.delete).toHaveBeenCalledWith(virtualEntity.id);
        });

        it('should key share accounting and best difficulty to the virtual identity row', async () => {
            const written: string[] = [];
            jest.spyOn(client as any, 'write').mockImplementation((data: string) => { written.push(data); return Promise.resolve(true); });

            emitMessage(MockRecording1.MINING_SUBSCRIBE);
            emitMessage(`{"id": 4, "method": "mining.suggest_difficulty", "params": [0]}`);
            emitMessage(MockRecording1.MINING_AUTHORIZE);
            await new Promise((r) => setTimeout(r, 100));

            emitMessage(`{"id": 7, "method": "mining.set_payout", "params": ["${NEW_PAYOUT_ADDRESS}", "o32"]}`);
            await new Promise((r) => setTimeout(r, 100));
            const jobAfter = lastNotifyParams(written)[0];

            const insertMock = clientService.insert as jest.Mock;
            const virtualEntity = await insertMock.mock.results[insertMock.mock.results.length - 1].value;
            expect(virtualEntity.clientName).toBe('o32');

            jest.useRealTimers();
            emitMessage(`{"id": 8, "method": "mining.submit", "params": ["${AUTHORIZED_ADDRESS}.bitaxe3", "${jobAfter}", "c708000000000001", "${MockRecording1.TIME}", "ed460d91", "00002000"]}`);
            await new Promise((r) => setTimeout(r, 1000));

            // the share is keyed to the virtual identity's row, so per-worker
            // session summaries aggregate per payout identity
            const recorded = shareAccountingService.recordAcceptedShare.mock.calls.map(call => call[0]);
            expect(recorded.length).toBe(1);
            expect(recorded[0].clientId).toBe(virtualEntity.id);
            expect(recorded[0].sessionId).toBe(MockRecording1.EXTRA_NONCE);

            // best difficulty ratchets on the virtual row and the payout address
            const bestCalls = (clientService.updateBestDifficultyIfHigher as jest.Mock).mock.calls;
            expect(bestCalls.some(call => call[0] === virtualEntity.id)).toBe(true);
            const addressBestCalls = (addressSettings.updateBestDifficultyIfHigher as jest.Mock).mock.calls;
            expect(addressBestCalls.some(call => call[0] === NEW_PAYOUT_ADDRESS)).toBe(true);
        });

    });


});
