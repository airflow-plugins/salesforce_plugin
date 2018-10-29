# Airflow Plugin - Salesforce
This plugin moves data from the Salesforce API to S3 based on the specified object

## Hooks
### Salesforce Hook
This hook handles the authentication and request to Salesforce. This extends the HttpHook and allows you to create a new connection to Salesforce, pull out data, and save it to a file.

### S3Hook
Core Airflow S3Hook with the standard boto dependency.

## Operators
### SalesforceToS3Operator
This operator composes the logic for this plugin. It queries the Salesforce Bulk API using a SOQL string and then drops results in an s3 bucket.
It accepts the following parameters:

    :param sf_conn_id:      Salesforce Connection Id
    :param soql:            Salesforce SOQL Query String used to query Bulk API
    :param: object_type:    Salesforce Object Type (lead, contact, etc)
    :param s3_conn_id:      S3 Connection Id
    :param s3_bucket:       S3 Bucket where query results will be put
    :param s3_key:          S3 Key that will be assigned to uploaded Salesforce
                            query results
### SalesforceSchematoRedshiftOperator

Reconciles schema between salesforce API objects and Redshift while leveraging the Salesforce API function to describe source objects and push the object attributes and datatypes to Redshift tables. Ignores Compound Fields as they are already broken out into their components elsewhere.
It accepts the following parameters:
``` 
   :param sf_conn_id:      The conn_id for your Salesforce Instance
   :param s3_conn_id:      The conn_id for your S3
   :param rs_conn_id:      The conn_id for your Redshift Instance
   :param sf_object:       The Salesforce object you wish you reconcile
                                the schema for.
                                Examples includes Lead, Contacts etc.
   :param rs_schema:       The schema where you want to put the renconciled
                                schema
   :param rs_table:        The table inside of the schema where you want to
                                put the renconciled schema
   :param s3_bucket:       The s3 bucket name that will be used to store the
                                JSONPath file to map source schema to redshift columns
   :param s3_key:          The s3 key that will be given to the JSONPath file
