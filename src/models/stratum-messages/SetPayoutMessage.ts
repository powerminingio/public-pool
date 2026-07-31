import { Expose, Transform } from 'class-transformer';
import { ArrayMaxSize, ArrayMinSize, IsArray, IsOptional, IsString, MaxLength } from 'class-validator';

import { eRequestMethod } from '../enums/eRequestMethod';
import { IsBitcoinAddress } from '../validators/bitcoin-address.validator';
import { StratumBaseMessage } from './StratumBaseMessage';

// mining.set_payout — switch the payout address this connection's coinbase pays,
// at runtime, without changing the connection's extranonce1. Subsequent jobs
// (and an immediate fresh job) pay the new address. Used by an upstream proxy to
// time-slice one connection across many payout identities. A normal miner never
// sends this and is unaffected.
//
// params: ["<address>", "<worker>"?]. The optional second element is a worker
// label for the payout identity (same role as the ".worker" suffix of
// mining.authorize): jobs carry it, accepted shares / found blocks are recorded
// under it, and a virtual worker presence per (address, worker) keeps the
// address page's workers list populated even though the identity has no
// connection of its own. Omitted ⇒ the connection's authorized worker (exactly
// the pre-worker-param behaviour).
export class SetPayoutMessage extends StratumBaseMessage {

    @IsArray()
    @ArrayMinSize(1)
    @ArrayMaxSize(2)
    @IsString({ each: true })
    params: string[];

    @Expose()
    @IsString()
    @Transform(({ value, key, obj, type }) => {
        return obj.params?.[0];
    })
    @IsBitcoinAddress()
    public address: string;

    @Expose()
    @IsOptional()
    @IsString()
    @MaxLength(64)
    @Transform(({ value, key, obj, type }) => {
        const worker = obj.params?.[1];
        // An empty label carries no information — treat it as absent.
        return worker === '' ? undefined : worker;
    })
    public worker?: string;

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
