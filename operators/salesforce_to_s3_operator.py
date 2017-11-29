from airflow.models import BaseOperator

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

    def __init__(self, sf_conn_id, soql, object_type, #sf config
                 s3_conn_id, s3_bucket, s3_key, #s3 config
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.sf_conn_id = sf_conn_id
        self.soql = soql
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.object = object_type[0].upper() + object_type[1:].lower()

    def execute(self, context):
        sf_conn = SalesforceHook(self.sf_conn_id).get_conn()
        s3_conn = S3Hook(self.s3_conn_id)

        logging.info(self.soql)
        query_results = sf_conn.bulk.__getattr__(self.object).query(self.soql)

        s3 = S3Hook(self.s3_conn_id)
        # One JSON Object Per Line
        query_results = [json.dumps(result, ensure_ascii=False) for result in query_results]
        query_results = '\n'.join(query_results)

        s3.load_string(query_results, self.s3_key, bucket_name=self.s3_bucket, replace=True)