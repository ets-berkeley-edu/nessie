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

CREATE EXTERNAL SCHEMA IF NOT EXISTS {redshift_schema_sis_advising_notes}
FROM data catalog
DATABASE '{redshift_schema_sis_advising_notes}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- advising notes historical snapshot (PS_SCI_NOTE_MAIN)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_notes'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_details historical snapshot (PS_SCI_NOTE_TRNDTL)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_note_details'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_topics historical snapshot (PS_SCI_NOTE_TOPIC)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_note_topics'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_priorities historical snapshot (PS_SCI_NOTE_PRITBL)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_note_priorities'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_attachments historical snapshot (PS_SCI_NOTE_ATTACH)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_note_attachments'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_attachment_data historical snapshot (PS_SCI_FILE_ATT)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_note_attachment_data'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_topic_config historical snapshot (PS_SCI_NOTETPC_TBL)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_note_topic_config'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_templates historical snapshot (PS_SCI_FREENOTETBL)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_note_templates'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_categories historical snapshot (PS_SAA_NOTE_TYPE)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_note_categories'
TABLE PROPERTIES ('skip.header.line.count'='1');

-- advising_note_subcategories historical snapshot (PS_SAA_NOTE_STYPE)
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
LOCATION '{loch_s3_sis_data_protected_path}/historical/advising-notes/advising_note_subcategories'
TABLE PROPERTIES ('skip.header.line.count'='1');
