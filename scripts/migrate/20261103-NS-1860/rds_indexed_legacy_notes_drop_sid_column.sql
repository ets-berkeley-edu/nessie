BEGIN TRANSACTION;

ALTER TABLE boac_advising_notes.advising_notes DROP COLUMN sid;

DROP INDEX IF EXISTS idx_advising_notes_sid;

COMMIT;
