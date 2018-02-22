from airflow.plugins_manager import AirflowPlugin
from hooks.salesforce_hook import SalesforceHook
from operators.salesforce_schema_to_redshift_operator import SalesforceSchemaToRedshiftOperator
from operators.salesforce_to_s3_operator import SalesforceBulkQueryToS3Operator
from operators.salesforce_to_s3_operator import SalesforceToS3Operator


class SalesforceToRedshiftPlugin(AirflowPlugin):
    name = "salesforce_to_redshift_plugin"
    hooks = [SalesforceHook]
    operators = [SalesforceToS3Operator,
                 SalesforceSchemaToRedshiftOperator,
                 SalesforceBulkQueryToS3Operator]
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []
