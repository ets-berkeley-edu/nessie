# Redshift connection parameters.

REDSHIFT_DATABASE = 'database'
REDSHIFT_HOST = 'redshift cluster'
REDSHIFT_PASSWORD = 'password'
REDSHIFT_PORT = 1234
REDSHIFT_USER = 'username'

# Schema names. Since Redshift instances are shared between environments, choose something that can
# be repeatedly created and torn down without conflicts.

REDSHIFT_SCHEMA_BOAC = 'testext_paulkerschen_boac'
REDSHIFT_SCHEMA_CANVAS = 'testext_paulkerschen_canvas'
