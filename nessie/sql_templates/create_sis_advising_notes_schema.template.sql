/**
 * Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.
 *
 * Permission to use, copy, modify, and distribute this software and its documentation
 * for educational, research, and not-for-profit purposes, without fee and without a
 * signed licensing agreement, is hereby granted, provided that the above copyright
 * notice, this paragraph and the following two paragraphs appear in all copies,
 * modifications, and distributions.
 *
 * Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
 * Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
 * http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.
 *
 * IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
 * SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
 * "AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 */

--------------------------------------------------------------------
-- CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

CREATE EXTERNAL SCHEMA {redshift_schema_sis_advising_notes}
FROM data catalog
DATABASE '{redshift_schema_sis_advising_notes}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- adivising notes (PS_SCI_NOTE_MAIN)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_notes
(
    sid VARCHAR,
    institution VARCHAR,
    note_id VARCHAR,
    appointment_id VARCHAR,
    note_category VARCHAR,
    note_subcategory VARCHAR,
    location VARCHAR,
    advisor_sid VARCHAR,
    operid VARCHAR,
    created_by VARCHAR,
    created_at VARCHAR,
    updated_by VARCHAR,
    updated_at VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_notes'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_details (PS_SCI_NOTE_TRNDTL)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_details
(
    sid VARCHAR,
    institution VARCHAR,
    note_id VARCHAR,
    note_seq_nr INT,
    note_priority VARCHAR,
    created_by VARCHAR,
    created_at VARCHAR,
    updated_by VARCHAR,
    updated_at VARCHAR,
    note_body VARCHAR(max)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\r'
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_note_details'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_topics (PS_SCI_NOTE_TOPIC)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_topics
(
    sid VARCHAR,
    note_id VARCHAR,
    note_topic VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_note_topics'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_priorities (PS_SCI_NOTE_PRITBL)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_priorities
(
    institution VARCHAR,
    note_priority VARCHAR,
    effective_date VARCHAR,
    effective_status VARCHAR,
    descr VARCHAR,
    descr_short VARCHAR,
    created_by VARCHAR,
    created_at VARCHAR,
    updated_by VARCHAR,
    updated_at VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_note_priorities'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_attachments (PS_SCI_NOTE_ATTACH)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_attachments
(
    sid VARCHAR,
    institution VARCHAR,
    note_id VARCHAR,
    attachment_seq_nr INT,
    descr VARCHAR,
    attachment_date VARCHAR,
    created_by VARCHAR,
    created_at VARCHAR,
    updated_by VARCHAR,
    updated_at VARCHAR,
    system_file_name VARCHAR,
    user_file_name VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_note_attachments'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_attachment_data (PS_SCI_FILE_ATT)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_attachment_data
(
    system_file_name VARCHAR,
    file_part_seq_nr INT,
    version INT,
    file_part_size INT,
    updated_at VARCHAR,
    updated_by VARCHAR,
    file_part_data VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_note_attachment_data'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_topic_config (PS_SCI_NOTETPC_TBL)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_topic_config
(
    institution VARCHAR,
    note_topic VARCHAR,
    effective_date VARCHAR,
    effective_status VARCHAR,
    descr VARCHAR,
    descr_short VARCHAR,
    created_by VARCHAR,
    created_at VARCHAR,
    updated_by VARCHAR,
    updated_at VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_note_topic_config'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_templates (PS_SCI_FREENOTETBL)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_templates
(
    institution VARCHAR,
    operid VARCHAR,
    note_id VARCHAR,
    template_title VARCHAR,
    created_by VARCHAR,
    created_at VARCHAR,
    updated_by VARCHAR,
    updated_at VARCHAR,
    note_body VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\r'
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_note_templates'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_categories (PS_SAA_NOTE_TYPE)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_categories
(
    institution VARCHAR,
    note_category VARCHAR,
    effective_date VARCHAR,
    effective_status VARCHAR,
    descr VARCHAR,
    descr_short VARCHAR,
    created_by VARCHAR,
    created_at VARCHAR,
    updated_by VARCHAR,
    updated_at VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_note_categories'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_subcategories (PS_SAA_NOTE_STYPE)
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_subcategories
(
    institution VARCHAR,
    note_category VARCHAR,
    note_subcategory VARCHAR,
    effective_date VARCHAR,
    effective_status VARCHAR,
    descr VARCHAR,
    descr_short VARCHAR,
    created_by VARCHAR,
    created_at VARCHAR,
    updated_by VARCHAR,
    updated_at VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_sis_advising_notes_data_path}/advising_note_subcategories'
TABLE PROPERTIES ('skip.header.line.count'='1');

--------------------------------------------------------------------
-- Internal schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_sis_advising_notes_internal} CASCADE;
CREATE SCHEMA {redshift_schema_sis_advising_notes_internal};

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE TABLE {redshift_schema_sis_advising_notes_internal}.advising_notes
INTERLEAVED SORTKEY (sid, note_id, note_seq_nr)
AS (
    SELECT
        N.sid,
        N.note_id,
        N.advisor_sid,
        N.appointment_id,
        C.descr AS note_category,
        S.descr AS note_subcategory,
        N.location,
        D.note_priority,
        D.note_body,
        N.operid,
        N.created_by,
        TO_TIMESTAMP(N.created_at, 'DD-MON-YY HH.MI.SS.US000 AM') AS created_at,
        N.updated_by,
        TO_TIMESTAMP(N.updated_at, 'DD-MON-YY HH.MI.SS.US000 AM') AS updated_at
    FROM
        {redshift_schema_sis_advising_notes}.advising_notes N
    JOIN
        {redshift_schema_sis_advising_notes}.advising_note_details D
    ON N.sid = D.sid
    AND N.institution = D.institution
    AND N.note_id = D.note_id
    LEFT OUTER JOIN
        {redshift_schema_sis_advising_notes}.advising_note_categories C
    ON N.note_category = C.note_category
    LEFT OUTER JOIN
        {redshift_schema_sis_advising_notes}.advising_note_subcategories S
    ON N.note_category = S.note_category
    AND N.note_subcategory = S.note_subcategory
    WHERE D.note_seq_nr = (
			SELECT MAX(M.note_seq_nr)
			FROM sis_advising_notes_ext_dev.advising_note_details M
			WHERE M.sid = D.sid
			AND M.institution = D.institution
			AND M.note_id = D.note_id
    )
);

CREATE TABLE {redshift_schema_sis_advising_notes_internal}.advising_note_attachments
INTERLEAVED SORTKEY (sid, note_id, attachment_seq_nr)
AS (
    SELECT
        sid,
        note_id,
        attachment_seq_nr,
        descr,
        TO_DATE(attachment_date, 'DD-MON-YY') AS attachment_date,
        created_by,
        TO_TIMESTAMP(created_at, 'DD-MON-YY HH.MI.SS.US000 AM') AS created_at,
        updated_by,
        TO_TIMESTAMP(updated_at, 'DD-MON-YY HH.MI.SS.US000 AM') AS updated_at,
        system_file_name,
        user_file_name,
        (sid || '_' || note_id || '_' || attachment_seq_nr || REGEXP_SUBSTR(system_file_name, '\\.[^.]*$')) AS sis_file_name
    FROM
        {redshift_schema_sis_advising_notes}.advising_note_attachments
);

CREATE TABLE {redshift_schema_sis_advising_notes_internal}.advising_note_topics
AS (
    SELECT
        sid,
        note_id,
        note_topic
    FROM
        {redshift_schema_sis_advising_notes}.advising_note_topics
);
