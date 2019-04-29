BEGIN TRANSACTION;

ALTER TABLE sis_data.enrolled_primary_sections
    ADD COLUMN sis_subject_area_compressed VARCHAR,
    ADD COLUMN sis_catalog_id VARCHAR;
CREATE INDEX IF NOT EXISTS enrolled_primary_sections_sis_subject_area_compressed_idx
    ON sis_data.enrolled_primary_sections (sis_subject_area_compressed);
CREATE INDEX IF NOT EXISTS enrolled_primary_sections_sis_catalog_id_idx
    ON sis_data.enrolled_primary_sections (sis_catalog_id);

COMMIT TRANSACTION;
