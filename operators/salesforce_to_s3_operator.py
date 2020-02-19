from tempfile import NamedTemporaryFile
import logging
import json

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook

from airflow.contrib.hooks.salesforce_hook import SalesforceHook


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
        sf_conn = SalesforceHook(self.sf_conn_id).get_conn()

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
    :param sf_fields:           *(optional)* list of fields that you want
                                to get from the object.
                                If *None*, then this will get all fields
                                for the object
    :type sf_fields:             list
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
    template_fields = ("s3_key",
                       "query")

    @apply_defaults
    def __init__(self,
                 sf_conn_id,
                 sf_obj,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,
                 sf_fields=None,
                 fmt="csv",
                 query=None,
                 relationship_object=None,
                 record_time_added=False,
                 coerce_to_timestamp=False,
                 *args,
                 **kwargs):

        super(SalesforceToS3Operator, self).__init__(*args, **kwargs)

        self.sf_conn_id = sf_conn_id
        self.object = sf_obj
        self.fields = sf_fields
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.fmt = fmt.lower()
        self.query = query
        self.relationship_object = relationship_object
        self.record_time_added = record_time_added
        self.coerce_to_timestamp = coerce_to_timestamp

    def special_query(self, query, sf_hook, relationship_object=None):
        if not query:
            raise ValueError("Query is None.  Cannot query nothing")

        sf_hook.sign_in()

        results = sf_hook.make_query(query)
        if relationship_object:
            records = []
            for r in results['records']:
                if r.get(relationship_object, None):
                    records.extend(r[relationship_object]['records'])
            results['records'] = records

        return results

    def execute(self, context):
        """
        Execute the operator.
        This will get all the data for a particular Salesforce model
        and write it to a file.
        """
        logging.info("Prepping to gather data from Salesforce")

        # Open a name temporary file to store output file until S3 upload
        with NamedTemporaryFile("w") as tmp:

            # Load the SalesforceHook
            hook = SalesforceHook(conn_id=self.sf_conn_id, output=tmp.name)

            # Attempt to login to Salesforce
            # If this process fails, it will raise an error and die.
            try:
                hook.sign_in()
            except:
                logging.debug('Unable to login.')

            # Get object from Salesforce
            # If fields were not defined, all fields are pulled.
            if not self.fields:
                self.fields = hook.get_available_fields(self.object)

            logging.info(
                "Making request for "
                "{0} fields from {1}".format(len(self.fields), self.object)
            )

            if self.query:
                query = self.special_query(self.query,
                                           hook,
                                           relationship_object=self.relationship_object
                                           )
            else:
                query = hook.get_object_from_salesforce(self.object,
                                                        self.fields)

            # output the records from the query to a file
            # the list of records is stored under the "records" key
            logging.info("Writing query results to: {0}".format(tmp.name))

            hook.write_object_to_file(query['records'],
                                      filename=tmp.name,
                                      fmt=self.fmt,
                                      coerce_to_timestamp=self.coerce_to_timestamp,
                                      record_time_added=self.record_time_added)

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
