DROP TABLE IF EXISTS migration_cdc_logs;
CREATE TABLE migration_cdc_logs(
    change_id bigserial,
    lsn_0 varchar PRIMARY KEY,
    lsn_1 varchar,
    data_size bigint,
    file_path varchar,
    received_at timestamp,
    proceeded_at timestamp
);
CREATE INDEX idx_change_id ON migration_cdc_logs(change_id);
CREATE INDEX idx_received_at ON migration_cdc_logs(received_at);
CREATE INDEX idx_proceeded_at ON migration_cdc_logs(proceeded_at);
