from airflow.plugins_manager import AirflowPlugin
from hooks.salesforce_hook import SalesforceHook
from operators.s3_to_redshift_operator import S3ToRedshiftOperator
from operators.salesforce_schema_to_redshift_operator import SalesforceSchemaToRedshiftOperator
from operators.salesforce_to_s3_operator import SalesforceBulkQueryToS3Operator

class SalesforceToRedshiftPlugin(AirflowPlugin):
    name = "salesforce_to_redshift_plugin"
    hooks = [SalesforceHook]
    operators = [S3ToRedshiftOperator, SalesforceSchemaToRedshiftOperator, SalesforceBulkQueryToS3Operator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []