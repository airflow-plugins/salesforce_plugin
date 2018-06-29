from airflow.plugins_manager import AirflowPlugin
from salesforce_plugin.hooks.salesforce_hook import SalesforceHook
from salesforce_plugin.operators.salesforce_schema_to_redshift_operator import SalesforceSchemaToRedshiftOperator
from salesforce_plugin.operators.salesforce_to_s3_operator import SalesforceBulkQueryToS3Operator
from salesforce_plugin.operators.salesforce_to_s3_operator import SalesforceToS3Operator


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
