/**
 * Copyright ©2021. The Regents of the University of California (Regents). All Rights Reserved.
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

CREATE EXTERNAL SCHEMA {redshift_schema_terms}
FROM data catalog
DATABASE '{redshift_schema_terms}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

CREATE OR REPLACE FUNCTION {redshift_schema_terms}.deduce_term_name(term_id VARCHAR)
RETURNS VARCHAR
STABLE
AS $$
    term_id = str(term_id)
    year = ('19' + term_id[1:3]) if term_id.startswith('1') else ('20' + term_id[1:3])
    terms = {
        '2': 'Spring',
        '5': 'Summer',
        '8': 'Fall',
        '0': 'Winter',
    }
    return (terms[term_id[3:4]] + ' ' + year)
$$ language plpythonu;

GRANT EXECUTE
ON function {redshift_schema_terms}.deduce_term_name(VARCHAR)
TO GROUP {redshift_app_boa_user}_group;

-- Terms
CREATE EXTERNAL TABLE {redshift_schema_terms}.term_definitions
SORTKEY (term_id)
AS (
    SELECT
      semester_year_term_cd AS term_id,
      {redshift_schema_terms}.deduce_term_name(semester_year_term_cd) AS term_name,
      session_begin_dt AS term_begins,
      session_end_dt AS term_ends,
      load_dt AS edl_load_date
    FROM {redshift_schema_edl_external}.student_academic_terms_session_data
    WHERE
      semester_year_term_cd >= {earliest_academic_history_term_id}
      AND academic_career_cd = 'UGRD'
    ORDER BY semester_year_term_cd, session_begin_dt
);

DROP FUNCTION {redshift_schema_terms}.deduce_term_name(VARCHAR);
