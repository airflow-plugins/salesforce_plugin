from tempfile import NamedTemporaryFile
import logging
import json
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook

# from airflow.contrib.hooks.salesforce_hook import SalesforceHook
from salesforce_plugin.hooks.salesforce_hook import SalesforceYieldingHook


class SalesforceBulkQueryToS3Operator(BaseOperator):
    """
        Queries the Salesforce Bulk API using a SOQL stirng. Results are then
        put into an S3 Bucket.

    :param sf_conn_id:      Salesforce Connection Id
    :param soql:            Salesforce SOQL Query String used to query Bulk API
    :param: object_type:    Salesforce Object Type (lead, contact, etc)
    :param s3_conn_id:      S3 Connection Id
    :param s3_bucket:       S3 Bucket where query results will be put
    :param s3_key:          S3 Key that will be assigned to uploaded Salesforce
                            query results
    """
    template_fields = ('soql', 's3_key')

    def __init__(self,
                 sf_conn_id,
                 soql,
                 object_type,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)

        self.sf_conn_id = sf_conn_id
        self.soql = soql
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.object = object_type[0].upper() + object_type[1:].lower()

    def execute(self, context):
        sf_conn = SalesforceYieldingHook(self.sf_conn_id).get_conn()

        logging.info(self.soql)
        query_results = sf_conn.bulk.__getattr__(self.object).query(self.soql)

        s3 = S3Hook(self.s3_conn_id)
        # One JSON Object Per Line
        query_results = [json.dumps(result, ensure_ascii=False) for result in query_results]
        query_results = '\n'.join(query_results)

        s3.load_string(query_results, self.s3_key, bucket_name=self.s3_bucket, replace=True)


class SalesforceToS3Operator(BaseOperator):
    """
    Salesforce to S3 Operator

    Makes a query against Salesforce and write the resulting data to a file.

    :param sf_conn_id:          Name of the Airflow connection that has
                                the following information:
                                    - username
                                    - password
                                    - security_token
    :type sf_conn_id:           string
    :param sf_obj:              Name of the relevant Salesforce object
    :param s3_conn_id:          The destination s3 connection id.
    :type s3_conn_id:           string
    :param s3_bucket:           The destination s3 bucket.
    :type s3_bucket:            string
    :param s3_key:              The destination s3 key.
    :type s3_key:               string
    :param fmt:                 *(optional)* format that the s3_key of the
                                data should be in. Possible values include:
                                    - csv
                                    - json
                                    - ndjson
                                *Default: csv*
    :type fmt:                  list
    :param query:               *(optional)* A specific query to run for
                                the given object.  This will override
                                default query creation.
                                *Default: None*
    :type query:                string
    :param query_fields:           *(optional)* list of fields that you want
                                to get from the object.
                                If *None*, then this will get all fields
                                for the object
    :type query_fields:             list
    :param query_scope:               *(optional)* A specific where clause scoping the query execution
                                the given object.  This will be appended to the query  with a where clause.
                                *Default: None*
    :type query:
    :param relationship_object: *(optional)* Some queries require
                                relationship objects to work, and
                                these are not the same names as
                                the SF object.  Specify that
                                relationship object here.
                                *Default: None*
    :type relationship_object:  string
    :param record_time_added:   *(optional)* True if you want to add a
                                Unix timestamp field to the resulting data
                                that marks when the data was
                                fetched from Salesforce.
                                *Default: False*.
    :type record_time_added:    string
    :param coerce_to_timestamp: *(optional)* True if you want to convert
                                all fields with dates and datetimes
                                into Unix timestamp (UTC).
                                *Default: False*.
    :type coerce_to_timestamp:  string
    """
    template_fields = ("s3_key","query","query_fields","query_scope")

    @apply_defaults
    def __init__(self,
                 sf_conn_id,
                 sf_obj,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 fmt="csv",
                 query=None,
                 query_fields=None,
                 query_scope=None,
                 relationship_object=None,
                 record_time_added=False,
                 coerce_to_timestamp=False,
                 stream=False,
                 *args,
                 **kwargs):

        super(SalesforceToS3Operator, self).__init__(*args, **kwargs)

        self.sf_conn_id = sf_conn_id
        self.object = sf_obj
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.fmt = fmt.lower()
        self.query = query
        self.query_fields = query_fields
        self.query_scope = query_scope
        self.relationship_object = relationship_object
        self.record_time_added = record_time_added
        self.coerce_to_timestamp = coerce_to_timestamp

    # def special_query(self, query, sf_hook, relationship_object=None):
    #     if not query:
    #         raise ValueError("Query is None.  Cannot query nothing")

    #     sf_hook.sign_in()

    #     results = sf_hook.make_query(query)
    #     if relationship_object:
    #         records = []
    #         for r in results['records']:
    #             if r.get(relationship_object, None):
    #                 records.extend(r[relationship_object]['records'])
    #         results['records'] = records

    #     return results

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Salesforce model
        and write it to a file.
        """
        logging.info("Prepping to gather data from Salesforce")

        # Open a name temporary file to store output file until S3 upload
        with NamedTemporaryFile("a") as tmp:

            hook = SalesforceYieldingHook(conn_id=self.sf_conn_id, output=tmp.name)

            # Attempt to login to Salesforce
            # If this process fails, it will raise an error and die.
            try:
                hook.sign_in()
            except:
                logging.debug('Unable to login.')

            # Get object from Salesforce
            # If fields were not defined, all fields are pulled.
            if not self.query_fields:
                self.query_fields = hook.get_available_fields(self.object)

            logging.info(
                "Making request for "
                "{0} fields from {1}".format(len(self.query_fields), self.object)
            )

            if self.query:
                self.soql = self.query
            else:
                self.soql = "SELECT {fields} FROM {object}".format(fields = ','.join(self.query_fields), object=self.object)

            if self.query_scope:
                self.soql += " WHERE {0}".format(self.query_scope)

            logging.debug("query: {0}".format(self.soql))


            for results,schema in hook.yield_all(self.soql, include_deleted=True):
                if self.relationship_object:
                    records = []
                    for r in results['records']:
                        if r.get(self.relationship_object, None):
                            records.extend(r[self.relationship_object]['records'])
                    results['records'] = records

                # output the records from the result to a file
                # the list of records is stored under the "records" key
                logging.info("Writing query results to: {0}".format(tmp.name))

                empty_set = [{'attributes':None}]
                result_set = results['records'] if len(results['records'])>0 else empty_set # patching result set to overcome an airflow.contrib hook bug
                data = hook.format_results(result_set,
                                           schema,
                                           fmt=self.fmt,
                                           coerce_to_timestamp=self.coerce_to_timestamp,
                                           record_time_added=self.record_time_added)

                tmp.write(data)
                tmp.write("\n")
                # Flush the temp file and upload temp file to S3
                tmp.flush()

            dest_s3 = S3Hook(self.s3_conn_id)

            dest_s3.load_file(
                filename=tmp.name,
                key=self.s3_key,
                bucket_name=self.s3_bucket,
                replace=True
            )

            tmp.close()

        logging.info("Query finished!")
