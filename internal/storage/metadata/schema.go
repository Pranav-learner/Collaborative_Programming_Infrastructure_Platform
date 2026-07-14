package metadata

// Table is the physical table name backing artifact metadata.
const Table = "storage_artifacts"

// Schema is the idempotent DDL for the artifact metadata system-of-record. It is
// applied by (*PostgresStore).Migrate. The design denormalizes expire_at and
// legal_hold out of the retention JSON so the cleanup reaper can scan expired
// artifacts with an index instead of a full-table JSON filter.
const Schema = `
CREATE TABLE IF NOT EXISTS storage_artifacts (
    id            TEXT PRIMARY KEY,
    object_key    TEXT        NOT NULL,
    bucket        TEXT        NOT NULL,
    content_hash  TEXT        NOT NULL,
    size          BIGINT      NOT NULL,
    content_type  TEXT        NOT NULL DEFAULT '',
    type          TEXT        NOT NULL,
    owner         TEXT        NOT NULL DEFAULT '',
    job_id        TEXT        NOT NULL DEFAULT '',
    room_id       TEXT        NOT NULL DEFAULT '',
    document_id   TEXT        NOT NULL DEFAULT '',
    language      TEXT        NOT NULL DEFAULT '',
    version       BIGINT      NOT NULL,
    lineage_id    TEXT        NOT NULL,
    is_latest     BOOLEAN     NOT NULL DEFAULT FALSE,
    compression   JSONB       NOT NULL DEFAULT '{}'::jsonb,
    encryption    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    retention     JSONB       NOT NULL DEFAULT '{}'::jsonb,
    state         TEXT        NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL,
    updated_at    TIMESTAMPTZ NOT NULL,
    deleted_at    TIMESTAMPTZ,
    expire_at     TIMESTAMPTZ,
    legal_hold    BOOLEAN     NOT NULL DEFAULT FALSE,
    metadata      JSONB       NOT NULL DEFAULT '{}'::jsonb,
    statistics    JSONB       NOT NULL DEFAULT '{}'::jsonb,
    cdn_metadata  JSONB       NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT storage_artifacts_lineage_version_uniq UNIQUE (lineage_id, version)
);

-- At most one head per lineage.
CREATE UNIQUE INDEX IF NOT EXISTS storage_artifacts_one_latest
    ON storage_artifacts (lineage_id) WHERE is_latest;

-- Object-level deduplication lookups.
CREATE INDEX IF NOT EXISTS storage_artifacts_dedup
    ON storage_artifacts (bucket, content_hash) WHERE state = 'available';

-- Reference counting before physical byte deletion.
CREATE INDEX IF NOT EXISTS storage_artifacts_object
    ON storage_artifacts (bucket, object_key);

-- Ownership / context queries.
CREATE INDEX IF NOT EXISTS storage_artifacts_owner ON storage_artifacts (owner);
CREATE INDEX IF NOT EXISTS storage_artifacts_job   ON storage_artifacts (job_id);
CREATE INDEX IF NOT EXISTS storage_artifacts_room  ON storage_artifacts (room_id);
CREATE INDEX IF NOT EXISTS storage_artifacts_type  ON storage_artifacts (type);

-- Retention reaper scan.
CREATE INDEX IF NOT EXISTS storage_artifacts_expiry
    ON storage_artifacts (state, expire_at) WHERE legal_hold = FALSE;
`

// columns is the canonical, ordered column list shared by SELECT/INSERT so the
// scan and bind sites never drift.
const columns = `id, object_key, bucket, content_hash, size, content_type, type,
	owner, job_id, room_id, document_id, language, version, lineage_id, is_latest,
	compression, encryption, retention, state, created_at, updated_at, deleted_at,
	expire_at, legal_hold, metadata, statistics, cdn_metadata`
