import { MigrationInterface, QueryRunner } from 'typeorm';

/**
 * Drops the legacy `client_statistics_entity` table left behind by an earlier
 * iteration of the TimescaleDB rework. Nothing in the current codebase reads
 * or writes it (statistics live in `accepted_share_entity` and the
 * `accepted_share_10m` rollups) — but its foreign key to `client_entity`
 * still vetoes ClientService.deleteOldClients(), which has been failing every
 * hour with FK violation 23503 on databases that carry the fossil. Soft-
 * deleted client rows therefore never purge and `client_entity` grows without
 * bound (observed live 2026-07-23, amplified by a session-churn incident).
 *
 * Databases that never had the legacy table are untouched (IF EXISTS).
 * No CASCADE on purpose: if anything unexpectedly still depends on the
 * table, the migration should fail loudly rather than drop it silently.
 */
export class DropLegacyClientStatistics1784837657000 implements MigrationInterface {
    public name = 'DropLegacyClientStatistics1784837657000';

    public async up(queryRunner: QueryRunner): Promise<void> {
        await queryRunner.query(`DROP TABLE IF EXISTS "client_statistics_entity"`);
    }

    public async down(queryRunner: QueryRunner): Promise<void> {
        // Intentional no-op: the table was an orphaned artifact and its data
        // (superseded by accepted_share_entity at the schema handover) is not
        // reconstructable. Nothing in the application requires it to exist.
    }
}
