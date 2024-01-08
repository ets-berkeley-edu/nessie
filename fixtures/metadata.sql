DROP SCHEMA IF EXISTS metadata CASCADE;

CREATE SCHEMA IF NOT EXISTS metadata;

DROP TYPE IF EXISTS merged_enrollment_term_job_status;
CREATE TYPE merged_enrollment_term_job_status AS ENUM ('created', 'started', 'success', 'error');

CREATE TABLE IF NOT EXISTS metadata.merged_enrollment_term_job_queue
(
    id SERIAL PRIMARY KEY,
    master_job_id VARCHAR NOT NULL,
    term_id VARCHAR NOT NULL,
    status merged_enrollment_term_job_status NOT NULL,
    details VARCHAR,
    instance_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS metadata.background_job_status
(
    job_id VARCHAR NOT NULL PRIMARY KEY,
    status VARCHAR NOT NULL,
    instance_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    details VARCHAR(4096)
);

CREATE TABLE IF NOT EXISTS metadata.canvas_sync_job_status
(
    job_id VARCHAR NOT NULL,
    filename VARCHAR NOT NULL,
    canvas_table VARCHAR NOT NULL,
    source_url VARCHAR(4096) NOT NULL,
    source_size BIGINT,
    destination_url VARCHAR(1024),
    destination_size BIGINT,
    status VARCHAR NOT NULL,
    details VARCHAR(4096),
    instance_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    PRIMARY KEY (job_id, filename)
);

CREATE TABLE IF NOT EXISTS metadata.canvas_synced_snapshots
(
    filename VARCHAR NOT NULL,
    canvas_table VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    size BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    deleted_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS metadata.registration_import_status
(
    sid VARCHAR NOT NULL PRIMARY KEY ,
    status VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS metadata.photo_import_status
(
    sid VARCHAR NOT NULL PRIMARY KEY,
    status VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL
);
