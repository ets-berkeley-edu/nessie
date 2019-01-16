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

CREATE EXTERNAL SCHEMA {redshift_schema_coe_external}
FROM data catalog
DATABASE '{redshift_schema_coe_external}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;


--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- advisor_student_mappings
CREATE EXTERNAL TABLE {redshift_schema_coe_external}.students(
    sid VARCHAR,
    advisor_ldap_uid VARCHAR,
    gender VARCHAR,
    ethnicity VARCHAR,
    minority VARCHAR,
    did_prep VARCHAR,
    prep_eligible VARCHAR,
    did_tprep VARCHAR,
    tprep_eligible VARCHAR,
    sat1read INT,
    sat1math INT,
    sat2math INT,
    in_met VARCHAR,
    grad_term VARCHAR,
    grad_year VARCHAR,
    probation VARCHAR,
    status VARCHAR
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '{loch_s3_coe_data_path}/students/';

--------------------------------------------------------------------
-- Internal Schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_coe} CASCADE;
CREATE SCHEMA {redshift_schema_coe};

--------------------------------------------------------------------
-- Internal Tables
--------------------------------------------------------------------

CREATE TABLE {redshift_schema_coe}.students
DISTKEY (advisor_ldap_uid)
INTERLEAVED SORTKEY (sid, advisor_ldap_uid)
AS (
    SELECT
    s.sid,
    s.advisor_ldap_uid,
    s.gender,
    s.ethnicity,
    (CASE WHEN s.minority = 'y' THEN true ELSE false END) AS minority,
    (CASE WHEN s.did_prep = 'y' THEN true ELSE false END) AS did_prep,
    (CASE WHEN s.prep_eligible = 'y' THEN true ELSE false END) AS prep_eligible,
    (CASE WHEN s.did_tprep = 'y' THEN true ELSE false END) AS did_tprep,
    (CASE WHEN s.tprep_eligible = 'y' THEN true ELSE false END) AS tprep_eligible,
    s.sat1read,
    s.sat1math,
    s.sat2math,
    (CASE WHEN s.in_met = 'y' THEN true ELSE false END) AS in_met,
    s.grad_term,
    s.grad_year,
    (CASE WHEN s.probation = 'y' THEN true ELSE false END) AS probation,
    s.status
    FROM {redshift_schema_coe_external}.students s
    -- Avoid header rows and other surprises by selecting numeric sids only.
    WHERE sid SIMILAR TO '[0-9]+'
);

CREATE TABLE {redshift_schema_coe}.student_profiles
(
    sid VARCHAR NOT NULL,
    profile VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);
