-- This script needs the "redshift_db" variable set before being run.
-- From the shell:
--    psql ...  --set=redshift_db="nessie_dev_redshift"
-- From within psql:
--    > \set redshift_db 'nessie_dev_redshift'

CREATE SCHEMA IF NOT EXISTS metadata;

CREATE TABLE IF NOT EXISTS metadata.background_job_status
(
    job_id VARCHAR NOT NULL PRIMARY KEY,
    status VARCHAR NOT NULL,
    instance_id VARCHAR,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    details VARCHAR(4096)
);

INSERT INTO metadata.background_job_status (
  SELECT *
  FROM dblink(:'redshift_db',$REDSHIFT$
    SELECT job_id, status, instance_id, created_at, updated_at, details
    FROM metadata.background_job_status
    WHERE created_at >= '2019-03-01'
  $REDSHIFT$) AS redshift_status (
    job_id VARCHAR,
    status VARCHAR,
    instance_id VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    details VARCHAR
  )
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

CREATE INDEX IF NOT EXISTS canvas_sync_job_idx
ON metadata.canvas_sync_job_status (job_id);

INSERT INTO metadata.canvas_sync_job_status (
  SELECT *
  FROM dblink(:'redshift_db',$REDSHIFT$
    SELECT job_id, filename, canvas_table, source_url, source_size, destination_url,
     destination_size, status, details, instance_id, created_at, updated_at
    FROM metadata.canvas_sync_job_status
    WHERE created_at >= '2019-03-01'
  $REDSHIFT$) AS redshift_status (
    job_id VARCHAR,
    filename VARCHAR,
    canvas_table VARCHAR,
    source_url VARCHAR,
    source_size BIGINT,
    destination_url VARCHAR,
    destination_size BIGINT,
    status VARCHAR,
    details VARCHAR,
    instance_id VARCHAR,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
  )
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

CREATE INDEX IF NOT EXISTS canvas_synced_snapshots_file_idx
ON metadata.canvas_synced_snapshots (filename);

INSERT INTO metadata.canvas_synced_snapshots (
  SELECT *
  FROM dblink(:'redshift_db',$REDSHIFT$
    SELECT filename, canvas_table, url, size, created_at, deleted_at
    FROM metadata.canvas_synced_snapshots
    WHERE created_at >= '2019-03-01'
  $REDSHIFT$) AS redshift_status (
    filename VARCHAR,
    canvas_table VARCHAR,
    url VARCHAR,
    size BIGINT,
    created_at TIMESTAMP,
    deleted_at TIMESTAMP
  )
);

CREATE TABLE IF NOT EXISTS metadata.registration_import_status
(
    sid VARCHAR NOT NULL PRIMARY KEY ,
    status VARCHAR NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

INSERT INTO metadata.registration_import_status (
  SELECT *
  FROM dblink(:'redshift_db',$REDSHIFT$
    SELECT sid, status, updated_at
    FROM metadata.registration_import_status
    WHERE term_id='all'
  $REDSHIFT$) AS redshift_status (
    sid VARCHAR,
    status VARCHAR,
    updated_at TIMESTAMP
  )
);
