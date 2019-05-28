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

-- advising notes incremental snapshot
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_notes_incr
(
    emplid VARCHAR,
    saa_note_id VARCHAR,
    saa_seq_nbr VARCHAR,
    advisor_id VARCHAR,
    sci_note_priority INT,
    saa_note_itm_long VARCHAR(max),
    scc_row_add_oprid VARCHAR,
    scc_row_add_dttm VARCHAR,
    scc_row_upd_oprid VARCHAR,
    scc_row_upd_dttm VARCHAR,
    sci_appt_id VARCHAR,
    saa_note_type VARCHAR,
    uc_adv_typ_desc VARCHAR,
    saa_note_subtype VARCHAR,
    uc_adv_subtyp_desc VARCHAR,
    sci_topic VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('strip.outer.array' = 'true')
LOCATION '{loch_s3_sis_data_protected_path}/sis-sysadm/daily/advising-notes/notes';

-- advising note attachments incremental snapshot
CREATE EXTERNAL TABLE {redshift_schema_sis_advising_notes}.advising_note_attachments_incr
(
    emplid VARCHAR,
    saa_note_id VARCHAR,
    userfilename VARCHAR,
    attachsysfilename VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('strip.outer.array' = 'true')
LOCATION '{loch_s3_sis_data_protected_path}/sis-sysadm/daily/advising-notes/note-attachments';

--------------------------------------------------------------------
-- Internal schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_sis_advising_notes_internal} CASCADE;
CREATE SCHEMA {redshift_schema_sis_advising_notes_internal};
GRANT USAGE ON SCHEMA {redshift_schema_sis_advising_notes_internal} TO GROUP {redshift_app_boa_user}_group;
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_sis_advising_notes_internal} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_sis_advising_notes_internal} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_sis_advising_notes_internal} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};

--------------------------------------------------------------------
-- Internal tables
--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {redshift_schema_sis_advising_notes_internal}.to_utc_iso_string(date_string VARCHAR, format_string VARCHAR)
RETURNS VARCHAR
STABLE
AS $$
  from datetime import datetime
  import pytz

  d = datetime.strptime(date_string, format_string)
  d = pytz.timezone('America/Los_Angeles').localize(d)
  return d.astimezone(pytz.utc).isoformat()
$$ language plpythonu;

GRANT EXECUTE
ON function {redshift_schema_sis_advising_notes_internal}.to_utc_iso_string(VARCHAR, VARCHAR)
TO GROUP {redshift_app_boa_user}_group;

CREATE TABLE {redshift_schema_sis_advising_notes_internal}.advising_notes
SORTKEY (id)
AS (
    SELECT DISTINCT
    	N.emplid || '-' || N.saa_note_id AS id,
    	N.emplid AS sid,
    	N.saa_note_id AS student_note_nr,
    	N.advisor_id AS advisor_sid,
    	N.sci_appt_id AS appointment_id,
    	N.uc_adv_typ_desc AS note_category,
    	N.uc_adv_subtyp_desc AS note_subcategory,
    	' ' as location,
    	COALESCE(CAST(N.sci_note_priority AS VARCHAR), ' ') AS note_priority,
    	N.saa_note_itm_long AS note_body,
    	N.scc_row_add_oprid AS operid,
    	N.scc_row_add_oprid AS created_by,
        TO_TIMESTAMP({redshift_schema_sis_advising_notes_internal}.to_utc_iso_string(N.scc_row_add_dttm, '%Y-%m-%dT%H:%M:%S.000Z'), 'YYYY-MM-DD"T"HH.MI.SS%z') AS created_at,
    	N.scc_row_upd_oprid AS updated_by,
        TO_TIMESTAMP({redshift_schema_sis_advising_notes_internal}.to_utc_iso_string(N.scc_row_upd_dttm, '%Y-%m-%dT%H:%M:%S.000Z'), 'YYYY-MM-DD"T"HH.MI.SS%z') AS updated_at
    FROM
    	{redshift_schema_sis_advising_notes}.advising_notes_incr N
    UNION
    SELECT
        N.sid || '-' || N.note_id AS id,
        N.sid,
        N.note_id AS student_note_nr,
        N.advisor_sid,
        N.appointment_id,
        C.descr AS note_category,
        S.descr AS note_subcategory,
        N.location,
        D.note_priority,
        D.note_body,
        N.operid,
        N.created_by,
        TO_TIMESTAMP({redshift_schema_sis_advising_notes_internal}.to_utc_iso_string(N.created_at, '%d-%b-%y %I.%M.%S.%f000 %p'), 'YYYY-MM-DD"T"HH.MI.SS%z') AS created_at,
        N.updated_by,
        TO_TIMESTAMP({redshift_schema_sis_advising_notes_internal}.to_utc_iso_string(D.updated_at, '%d-%b-%y %I.%M.%S.%f000 %p'), 'YYYY-MM-DD"T"HH.MI.SS%z') AS updated_at
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
    JOIN (
    		SELECT MAX(note_seq_nr) as max_seq_nr, sid, institution, note_id
            FROM {redshift_schema_sis_advising_notes}.advising_note_details
			GROUP BY sid, institution, note_id
	) AS M(max_seq_nr, sid, institution, note_id) 
    ON M.sid = N.sid
    AND M.institution = N.institution
    AND M.note_id = N.note_id
    AND M.max_seq_nr  = D.note_seq_nr
);

CREATE TABLE {redshift_schema_sis_advising_notes_internal}.advising_note_attachments
INTERLEAVED SORTKEY (advising_note_id, sis_file_name)
AS (
    SELECT
    	A.emplid || '-' || N.saa_note_id AS advising_note_id,
    	A.emplid AS sid,
    	A.saa_note_id AS student_note_nr,
        N.scc_row_add_oprid AS created_by,
        A.userfilename AS user_file_name,
        A.attachsysfilename AS sis_file_name
    FROM
        {redshift_schema_sis_advising_notes}.advising_note_attachments_incr A
    JOIN
        {redshift_schema_sis_advising_notes}.advising_notes_incr N
    ON A.emplid = N.emplid
    AND A.saa_note_id = N.saa_note_id
    UNION
    SELECT
        sid || '-' || note_id AS advising_note_id,
        sid,
        note_id AS student_note_nr,
        created_by,
        user_file_name,
        (sid || '_' || note_id || '_' || attachment_seq_nr || REGEXP_SUBSTR(system_file_name, '\\.[^.]*$')) AS sis_file_name
    FROM
        {redshift_schema_sis_advising_notes}.advising_note_attachments
);

CREATE TABLE {redshift_schema_sis_advising_notes_internal}.advising_note_topics
SORTKEY (advising_note_id)
AS (
	SELECT
		emplid || '-' || saa_note_id AS advising_note_id,
		emplid AS sid,
    	saa_note_id AS student_note_nr,
    	sci_topic AS note_topic
	FROM {redshift_schema_sis_advising_notes}.advising_notes_incr
	UNION
    SELECT
        sid || '-' || note_id AS advising_note_id,
        sid,
        note_id AS student_note_nr,
        note_topic
    FROM
        {redshift_schema_sis_advising_notes}.advising_note_topics
);

DROP FUNCTION {redshift_schema_sis_advising_notes_internal}.to_utc_iso_string(VARCHAR, VARCHAR);
