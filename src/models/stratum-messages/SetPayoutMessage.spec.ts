import { plainToInstance } from 'class-transformer';

import { eRequestMethod } from '../enums/eRequestMethod';
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
    });

});
