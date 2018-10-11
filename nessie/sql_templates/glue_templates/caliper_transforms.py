"""
Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research, and not-for-profit purposes, without fee and without a
signed licensing agreement, is hereby granted, provided that the above copyright
notice, this paragraph and the following two paragraphs appear in all copies,
modifications, and distributions.

Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.

IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
"AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.
"""

import json
import sys
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
import boto3
from pyspark.context import SparkContext
from pyspark.sql.types import StructType


# Pyspark Glue still uses python 2.7 on the AWS cluster while Nessie is running python on 3.6.
args = getResolvedOptions(
    sys.argv,
    [
        'JOB_NAME',
        'LRS_INCREMENTAL_TRANSIENT_BUCKET',
        'LRS_CANVAS_CALIPER_SCHEMA_PATH',
        'LRS_CANVAS_CALIPER_INPUT_DATA_PATH',
        'LRS_GLUE_TEMP_DIR',
        'LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH',
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

lrs_transient_bucket = args['LRS_INCREMENTAL_TRANSIENT_BUCKET']
lrs_glue_temp_dir = args['LRS_GLUE_TEMP_DIR']
lrs_caliper_schema_path = args['LRS_CANVAS_CALIPER_SCHEMA_PATH']
lrs_canvas_caliper_input_path = args['LRS_CANVAS_CALIPER_INPUT_DATA_PATH']
lrs_canvas_caliper_explode_path = args['LRS_CANVAS_CALIPER_EXPLODE_OUTPUT_PATH']


# Import prepared canvas caliper json schema and convert to struct type that can be applied to spark dataframe as template
def import_caliper_schema(bucket, key):
    s3 = boto3.client('s3', region_name='us-west-2')
    json_file = s3.get_object(Bucket=bucket, Key=key)
    json_object = json.load(json_file['Body'])
    schema_struct = StructType.fromJson(json_object)
    return schema_struct


# Relationalizes spark dataframe and exports to s3
def relationalize_and_export(statements_df):
    # convert spark dataframe to glue dynamic frame
    statement_dynamic_frame = DynamicFrame.fromDF(statements_df, glueContext, 'statement_dynamic_frame')
    glue_temp_dir = 's3://{}/{}'.format(lrs_transient_bucket, lrs_glue_temp_dir)
    # transform the dataframe using glue relationalize
    statement_explode_df = statement_dynamic_frame.relationalize('root', glue_temp_dir)
    statement_explode_df.keys()

    lrs_explode_output_path = 's3://{}/{}'.format(lrs_transient_bucket, lrs_canvas_caliper_explode_path)
    # write glue dynamic frame contents to s3 location as compressed json gzip files
    glueContext.write_dynamic_frame.from_options(
        frame=statement_explode_df,
        connection_type='s3',
        connection_options={'path': lrs_explode_output_path, 'compression': 'gzip'},
        format='json',
        transformation_ctx='datasink',
    )
    return


# Create a caliper schema struct that can be used as a template to create spark dataframes
# print 'Importing Caliper corrected schema from S3 and convert it to struct type'
caliper_schema_struct = import_caliper_schema(
    args['LRS_INCREMENTAL_TRANSIENT_BUCKET'],
    args['LRS_CANVAS_CALIPER_SCHEMA_PATH'],
)

# Apply prepared schema template on the incoming statements to create a spark dataframe
sys.stdout.write('Importing Caliper statements from S3 with the corrected schema')
lrs_caliper_input_data_path = 's3://{}/{}'.format(lrs_transient_bucket, lrs_canvas_caliper_input_path)
statements_df = spark.read.schema(caliper_schema_struct).json(lrs_caliper_input_data_path)
statements_df.printSchema()

# Verify inferred schema from spark process.
sys.stdout.write('Display inferred schema from the dataframe')
schema_json = statements_df.schema.json()
sys.stdout.write(schema_json)

# Convert the data to flat tables and export to S3 as compressed json gzip files.
sys.stdout.write('Exporting dynamic frame as json partitions in S3')
relationalize_and_export(statements_df)

job.commit()
