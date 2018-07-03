/**
 * Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.
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

CREATE EXTERNAL SCHEMA {redshift_schema_asc_external}
FROM data catalog
DATABASE '{redshift_schema_asc_external}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- athletics
CREATE EXTERNAL TABLE {redshift_schema_asc_external}.athletics(
    group_code VARCHAR,
    group_name VARCHAR,
    team_code VARCHAR,
    team_name VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('paths'='group_code, group_name, team_code, team_name')
LOCATION '{loch_s3_asc_data_path}/athletics/';

-- students
CREATE EXTERNAL TABLE {redshift_schema_asc_external}.students(
    sid VARCHAR,
    in_intensive_cohort BOOLEAN,
    is_active_asc BOOLEAN,
    status_asc VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('paths'='sid, in_intensive_cohort, is_active_asc, status_asc')
LOCATION '{loch_s3_asc_data_path}/students/';

-- student_athletes
CREATE EXTERNAL TABLE {redshift_schema_asc_external}.student_athletes(
    group_code VARCHAR,
    sid VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('paths'='group_code, sid')
LOCATION '{loch_s3_asc_data_path}/student_athletes/';

--------------------------------------------------------------------
-- Internal Schema
--------------------------------------------------------------------

DROP SCHEMA IF EXISTS {redshift_schema_asc} CASCADE;
CREATE SCHEMA {redshift_schema_asc};

--------------------------------------------------------------------
-- Internal Tables
--------------------------------------------------------------------

CREATE TABLE {redshift_schema_asc}.students
DISTKEY (group_code)
INTERLEAVED SORTKEY (sid, intensive, active, group_code)
AS (
    SELECT
    s.sid,
    s.in_intensive_cohort AS intensive,
    s.is_active_asc AS active,
    s.status_asc,
    a.group_code,
    a.group_name,
    a.team_code,
    a.team_name
    FROM {redshift_schema_asc_external}.students s
    JOIN {redshift_schema_asc_external}.student_athletes sa
       ON s.sid = sa.sid
    JOIN {redshift_schema_asc_external}.athletics a
       ON sa.group_code = a.group_code
);

CREATE TABLE {redshift_schema_asc}.student_profiles
(
    sid VARCHAR NOT NULL,
    profile VARCHAR(max) NOT NULL
)
DISTKEY (sid)
SORTKEY (sid);
