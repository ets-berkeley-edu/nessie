/**
 * Copyright ©2020. The Regents of the University of California (Regents). All Rights Reserved.
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

CREATE SCHEMA IF NOT EXISTS {rds_schema_oua};
GRANT USAGE ON SCHEMA {rds_schema_oua} TO {rds_app_boa_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_oua} GRANT SELECT ON TABLES TO {rds_app_boa_user};

BEGIN TRANSACTION;

DROP TABLE IF EXISTS {rds_schema_oua}.student_admits CASCADE;

CREATE TABLE {rds_schema_oua}.student_admits (
  applyuc_cpid VARCHAR NOT NULL,
  cs_empl_id VARCHAR NOT NULL,
  freshman_or_transfer VARCHAR,
  admit_status VARCHAR,
  current_sir VARCHAR,
  college VARCHAR,
  first_name VARCHAR,
  last_name VARCHAR,
  birthdate VARCHAR,
  email VARCHAR,
  daytime VARCHAR,
  mobile VARCHAR,
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
  first_generation_student VARCHAR,
  first_generation_college VARCHAR,
  parent_1_education_level VARCHAR,
  parent_2_education_level VARCHAR,
  hs_unweighted_gpa VARCHAR,
  hs_weighted_gpa VARCHAR,
  transfer_gpa VARCHAR,
  act_composite DOUBLE PRECISION,
  act_math DOUBLE PRECISION,
  act_english DOUBLE PRECISION,
  act_reading DOUBLE PRECISION,
  act_writing DOUBLE PRECISION,
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
  special_program_cep VARCHAR
);

INSERT INTO {rds_schema_oua}.student_admits (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
    SELECT DISTINCT
      applyuc_cpid,
      cs_empl_id,
      freshman_or_transfer,
      admit_status,
      current_sir,
      college,
      first_name,
      last_name,
      birthdate,
      email,
      daytime,
      mobile,
      permanent_street_1,
      permanent_street_2,
      permanent_city,
      permanent_region,
      permanent_postal,
      permanent_country,
      sex,
      gender_identity,
      xethnic,
      hispanic,
      urem,
      first_generation_student,
      first_generation_college,
      parent_1_education_level,
      parent_2_education_level,
      hs_unweighted_gpa,
      hs_weighted_gpa,
      transfer_gpa,
      act_composite,
      act_math,
      act_english,
      act_reading,
      act_writing,
      sat_total,
      sat_r_evidence_based_rw_section,
      sat_r_math_section,
      sat_r_essay_reading,
      sat_r_essay_analysis,
      sat_r_essay_writing,
      application_fee_waiver_flag,
      foster_care_flag,
      family_is_single_parent,
      student_is_single_parent,
      family_dependents_num,
      student_dependents_num,
      family_income,
      student_income,
      is_military_dependent,
      military_status,
      reentry_status,
      athlete_status,
      summer_bridge_status,
      last_school_lcff_plus_flag,
      special_program_cep
    FROM {redshift_schema_oua}.admissions
    ORDER BY cs_empl_id
  $REDSHIFT$)
  AS redshift_student_admits (
    applyuc_cpid VARCHAR,
    cs_empl_id VARCHAR,
    freshman_or_transfer VARCHAR,
    admit_status VARCHAR,
    current_sir VARCHAR,
    college VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    birthdate VARCHAR,
    email VARCHAR,
    daytime VARCHAR,
    mobile VARCHAR,
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
    first_generation_student VARCHAR,
    first_generation_college VARCHAR,
    parent_1_education_level VARCHAR,
    parent_2_education_level VARCHAR,
    hs_unweighted_gpa VARCHAR,
    hs_weighted_gpa VARCHAR,
    transfer_gpa VARCHAR,
    act_composite DOUBLE PRECISION,
    act_math DOUBLE PRECISION,
    act_english DOUBLE PRECISION,
    act_reading DOUBLE PRECISION,
    act_writing DOUBLE PRECISION,
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
    special_program_cep VARCHAR
  )
);

CREATE INDEX idx_oua_student_admits_cs_empl_id ON {rds_schema_oua}.student_admits(cs_empl_id);
CREATE INDEX idx_oua_student_admits_slate_id ON {rds_schema_oua}.student_admits(applyuc_cpid);
CREATE INDEX idx_oua_student_admits_names ON {rds_schema_oua}.student_admits(first_name, last_name);
CREATE INDEX idx_oua_student_admits_hispanic ON {rds_schema_oua}.student_admits(gender_identity);
CREATE INDEX idx_oua_student_admits_xethnic ON {rds_schema_oua}.student_admits(xethnic);
CREATE INDEX idx_oua_student_admits_urem ON {rds_schema_oua}.student_admits(urem);
CREATE INDEX idx_oua_student_admits_current_sir ON {rds_schema_oua}.student_admits(current_sir);
CREATE INDEX idx_oua_student_admits_admit_status ON {rds_schema_oua}.student_admits(admit_status);
CREATE INDEX idx_oua_student_admits_freshman ON {rds_schema_oua}.student_admits(freshman_or_transfer);
CREATE INDEX idx_oua_student_admits_college ON {rds_schema_oua}.student_admits(college);
CREATE INDEX idx_oua_student_admits_transfer_gpa ON {rds_schema_oua}.student_admits(transfer_gpa);
CREATE INDEX idx_oua_student_admits_parent_education_level ON {rds_schema_oua}.student_admits(parent_1_education_level, parent_2_education_level);
CREATE INDEX idx_oua_student_admits_student_single_parent ON {rds_schema_oua}.student_admits(student_is_single_parent);
CREATE INDEX idx_oua_student_admits_family_single_parent ON {rds_schema_oua}.student_admits(family_is_single_parent);
CREATE INDEX idx_oua_student_admits_family_income ON {rds_schema_oua}.student_admits(family_income);

COMMIT TRANSACTION;
