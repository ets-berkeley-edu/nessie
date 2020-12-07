/**
 * Copyright ©2021. The Regents of the University of California (Regents). ALL Rights Reserved.
 *
 * PermissiON TO use, copy, modify, and distribute this software and its documentatiON
 * for educatiONal, research, and not-for-profit purposes, without fee and without a
 * signed licensing agreement, is hereby GRANTed, provided that the above copyright
 * notice, this paragraph and the following two paragraphs appear IN ALL copies,
 * modificatiONs, and distributiONs.
 *
 * CONtact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
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


REVOKE CREATE ON SCHEMA public FROM GROUP {redshift_app_boa_user}_group;
REVOKE CREATE ON SCHEMA {redshift_schema_asc} FROM GROUP {redshift_app_boa_user}_group;
REVOKE CREATE ON SCHEMA {redshift_schema_coe} FROM GROUP {redshift_app_boa_user}_group;
REVOKE CREATE ON SCHEMA {redshift_schema_sis_advising_notes_internal} FROM GROUP {redshift_app_boa_user}_group;
REVOKE CREATE ON SCHEMA {redshift_schema_boac} FROM GROUP {redshift_app_boa_user}_group;
REVOKE CREATE ON SCHEMA {redshift_schema_intermediate} FROM GROUP {redshift_app_boa_user}_group;
REVOKE CREATE ON SCHEMA {redshift_schema_student} FROM GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_asc} TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_coe} TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_sis_advising_notes_internal} TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_boac} TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_intermediate} TO GROUP {redshift_app_boa_user}_group;
GRANT USAGE ON SCHEMA {redshift_schema_student} TO GROUP {redshift_app_boa_user}_group;
GRANT SELECT ON ALL TABLES IN SCHEMA {redshift_schema_asc} TO GROUP {redshift_app_boa_user}_group;
GRANT SELECT ON ALL TABLES IN SCHEMA {redshift_schema_coe} TO GROUP {redshift_app_boa_user}_group;
GRANT SELECT ON ALL TABLES IN SCHEMA {redshift_schema_sis_advising_notes_internal} TO GROUP {redshift_app_boa_user}_group;
GRANT SELECT ON ALL TABLES IN SCHEMA {redshift_schema_boac} TO GROUP {redshift_app_boa_user}_group;
GRANT SELECT ON ALL TABLES IN SCHEMA {redshift_schema_intermediate} TO GROUP {redshift_app_boa_user}_group;
GRANT SELECT ON ALL TABLES IN SCHEMA {redshift_schema_student} TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_asc} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_coe} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_sis_advising_notes_internal} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_boac} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_intermediate} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
ALTER default PRIVILEGES IN SCHEMA {redshift_schema_student} GRANT SELECT ON TABLES TO GROUP {redshift_app_boa_user}_group;
