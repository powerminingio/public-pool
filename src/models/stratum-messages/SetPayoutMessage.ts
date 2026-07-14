import { Expose, Transform } from 'class-transformer';
import { ArrayMaxSize, ArrayMinSize, IsArray, IsString } from 'class-validator';

import { eRequestMethod } from '../enums/eRequestMethod';
import { IsBitcoinAddress } from '../validators/bitcoin-address.validator';
import { StratumBaseMessage } from './StratumBaseMessage';

// mining.set_payout — switch the payout address this connection's coinbase pays,
// at runtime, without changing the connection's extranonce1. Subsequent jobs
// (and an immediate fresh job) pay the new address. Used by an upstream proxy to
// time-slice one connection across many payout identities. A normal miner never
// sends this and is unaffected.
export class SetPayoutMessage extends StratumBaseMessage {

    @IsArray()
    @ArrayMinSize(1)
    @ArrayMaxSize(1)
    @IsString({ each: true })
    params: string[];

    @Expose()
    @IsString()
    @Transform(({ value, key, obj, type }) => {
        return obj.params?.[0];
    })
    @IsBitcoinAddress()
    public address: string;

    constructor() {
        super();
        this.method = eRequestMethod.SET_PAYOUT;
    }

    public response() {
        return {
            id: this.id,
            error: null,
            result: true
        };
    }
}
