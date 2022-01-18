/**
 * Copyright ©2022. The Regents of the University of California (Regents). All Rights Reserved.
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
CREATE EXTERNAL SCHEMA {redshift_schema_oua}
FROM data catalog
DATABASE '{redshift_schema_oua}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------
-- Office of Ungergraduate Admissions data for CE3
CREATE EXTERNAL TABLE {redshift_schema_oua}.admissions
(
    applyuc_cpid VARCHAR,
    cs_empl_id VARCHAR,
    residency_category VARCHAR,
    freshman_or_transfer VARCHAR,
    admit_term VARCHAR,
    admit_status VARCHAR,
    current_sir VARCHAR,
    college VARCHAR,
    first_name VARCHAR,
    middle_name VARCHAR,
    last_name VARCHAR,
    birthdate VARCHAR,
    daytime_phone VARCHAR,
    mobile VARCHAR,
    email VARCHAR,
    campus_email_1 VARCHAR,
    permanent_street_1 VARCHAR,
    permanent_street_2 VARCHAR,
    permanent_city VARCHAR,
    permanent_region VARCHAR,
    permanent_postal VARCHAR,
    permanent_country VARCHAR,
    sex VARCHAR,
    gender_identity VARCHAR,
    xethnic VARCHAR,
    hispanic VARCHAR,
    urem VARCHAR,
    first_generation_college VARCHAR,
    parent_1_education_level VARCHAR,
    parent_2_education_level VARCHAR,
    highest_parent_education_level VARCHAR,
    hs_unweighted_gpa VARCHAR,
    hs_weighted_gpa VARCHAR,
    transfer_gpa VARCHAR,
    act_composite INTEGER,
    act_math INTEGER,
    act_english INTEGER,
    act_reading INTEGER,
    act_writing INTEGER,
    sat_total INTEGER,
    sat_r_evidence_based_rw_section INTEGER,
    sat_r_math_section INTEGER,
    sat_r_essay_reading INTEGER,
    sat_r_essay_analysis INTEGER,
    sat_r_essay_writing INTEGER,
    application_fee_waiver_flag VARCHAR,
    foster_care_flag VARCHAR,
    family_is_single_parent VARCHAR,
    student_is_single_parent VARCHAR,
    family_dependents_num VARCHAR,
    student_dependents_num VARCHAR,
    family_income VARCHAR,
    student_income VARCHAR,
    is_military_dependent VARCHAR,
    military_status VARCHAR,
    reentry_status VARCHAR,
    athlete_status VARCHAR,
    summer_bridge_status VARCHAR,
    last_school_lcff_plus_flag VARCHAR,
    special_program_cep VARCHAR,
    us_citizenship_status VARCHAR,
    us_non_citizen_status VARCHAR,
    citizenship_country VARCHAR,
    permanent_residence_country VARCHAR,
    non_immigrant_visa_current VARCHAR,
    non_immigrant_visa_planned VARCHAR,
    uid VARCHAR
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '\"',
  'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '{loch_s3_oua_data_path}/admissions'
TABLE PROPERTIES (
    'skip.header.line.count'='1'
);

-- Provisions DB Link group with permissions to query OUA external schema and associated tables
GRANT USAGE ON SCHEMA {redshift_schema_oua} TO GROUP {redshift_dblink_group};
ALTER DEFAULT PRIVILEGES IN SCHEMA {redshift_schema_oua} GRANT SELECT ON TABLES TO GROUP {redshift_dblink_group};
