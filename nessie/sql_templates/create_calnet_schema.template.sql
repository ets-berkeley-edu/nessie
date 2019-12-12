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

--------------------------------------------------------------------
-- CREATE EXTERNAL SCHEMA
--------------------------------------------------------------------

CREATE EXTERNAL SCHEMA {redshift_schema_calnet}
FROM data catalog
DATABASE '{redshift_schema_calnet}'
IAM_ROLE '{redshift_iam_role}'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

--------------------------------------------------------------------
-- External Tables
--------------------------------------------------------------------

-- persons
CREATE EXTERNAL TABLE {redshift_schema_calnet}.persons(
    sid VARCHAR,
    ldap_uid VARCHAR,
    first_name VARCHAR,
    last_name VARCHAR,
    campus_email VARCHAR,
    email VARCHAR,
    affiliations VARCHAR
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ('paths'='sid, ldap_uid, first_name, last_name, campus_email, email, affiliations')
LOCATION '{loch_s3_calnet_data_path}/advisees';

-- Output our full SID list to S3 for use by clients with S3-only access such as Junction.
UNLOAD (
    'SELECT sid
    FROM {redshift_schema_calnet}.persons'
)
TO '{sid_snapshot_path}/snapshot'
IAM_ROLE '{redshift_iam_role}'
ENCRYPTED
NULL AS ''
DELIMITER AS '\t'
PARALLEL OFF
ALLOWOVERWRITE;
