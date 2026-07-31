import { plainToInstance } from 'class-transformer';
import { validate } from 'class-validator';

import { eRequestMethod } from '../enums/eRequestMethod';

// The address validator needs the Nest DI container for its ConfigService
// (main.ts registers it via useContainer); outside the app it can't run, so
// unit tests no-op it — same pattern as StratumV1Client.spec. The worker/array
// constraints under test here are plain class-validator decorators and run for
// real.
jest.mock('../validators/bitcoin-address.validator', () => ({
    IsBitcoinAddress() {
        return jest.fn();
    },
}));

// eslint-disable-next-line import/first
import { SetPayoutMessage } from './SetPayoutMessage';

describe('SetPayoutMessage', () => {

    describe('test message parsing', () => {

        const SET_PAYOUT_MESSAGE = '{"id": 7, "method": "mining.set_payout", "params": ["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4"]}';

        const message = plainToInstance(
            SetPayoutMessage,
            JSON.parse(SET_PAYOUT_MESSAGE),
        );

        it('should parse the payout address from params[0]', () => {
            expect(message.id).toEqual(7);
            expect(message.method).toEqual(eRequestMethod.SET_PAYOUT);
            expect(message.address).toEqual('tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4');
        });

        it('should build a success response carrying the request id', () => {
            expect(message.response()).toEqual({ id: 7, error: null, result: true });
        });

        it('should leave the worker undefined when only the address is sent', async () => {
            expect(message.worker).toBeUndefined();
            expect((await validate(message)).length).toBe(0);
        });
    });

    describe('optional worker param', () => {

        const parse = (params: string) => plainToInstance(
            SetPayoutMessage,
            JSON.parse(`{"id": 7, "method": "mining.set_payout", "params": ${params}}`),
        );

        it('should parse the worker from params[1]', async () => {
            const message = parse('["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4", "o32"]');
            expect(message.address).toEqual('tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4');
            expect(message.worker).toEqual('o32');
            expect((await validate(message)).length).toBe(0);
        });

        it('should treat an empty worker as absent', async () => {
            const message = parse('["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4", ""]');
            expect(message.worker).toBeUndefined();
            expect((await validate(message)).length).toBe(0);
        });

        it('should reject a non-string worker', async () => {
            const message = parse('["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4", 42]');
            expect((await validate(message)).length).toBeGreaterThan(0);
        });

        it('should reject a worker longer than 64 characters', async () => {
            const message = parse(`["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4", "${'w'.repeat(65)}"]`);
            expect((await validate(message)).length).toBeGreaterThan(0);
        });

        it('should reject more than two params', async () => {
            const message = parse('["tb1qumezefzdeqqwn5zfvgdrhxjzc5ylr39uhuxcz4", "o32", "extra"]');
            expect((await validate(message)).length).toBeGreaterThan(0);
        });
    });

    describe('malformed messages', () => {

        it('should not throw when params are missing entirely', () => {
            const malformed = plainToInstance(
                SetPayoutMessage,
                JSON.parse('{"id": 8, "method": "mining.set_payout"}'),
            );
            expect(malformed.address).toBeUndefined();
        });

        it('should not throw when params is null', () => {
            const malformed = plainToInstance(
                SetPayoutMessage,
                JSON.parse('{"id": 8, "method": "mining.set_payout", "params": null}'),
            );
            expect(malformed.address).toBeUndefined();
        });
    });

});
