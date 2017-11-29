from airflow.models import BaseOperator

class SalesforceSchemaToRedshiftOperator(BaseOperator):
    """
    Reconcile Schema between salesforce API objects and Redshift

        Leverages the Salesforce API function to describe source objects
        and push the object attributes and datatypes to Redshift tables
        1 table per object

        Ignores Compound Fields as they are already broken out into their
        components else where in the
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

        .. note::
            Be aware that JSONPath files are used for the column mapping of source
            objects to destination tables

            Datatype conversiona happen via the dt_conv dictionary
    """

    dt_conv = {
        'boolean': lambda x: 'boolean',
        'date': lambda x: 'date',
        'dateTime': lambda x: 'TIMESTAMP',
        'double': lambda x: 'float8',
        'email': lambda x: 'varchar(80)',
        'id': lambda x: 'varchar(100)',
        'ID': lambda x: 'varchar(100)',
        'int': lambda x: 'int',
        'picklist': lambda x: 'varchar({})'.format(x if x <= 65535 else 'MAX'),
        'phone': lambda x: 'varchar(40)',
        'string': lambda x: 'varchar({})'.format(x if x <= 65535 else 'MAX'),
        'textarea': lambda x: 'varchar({})'.format(x if x <= 65535 else 'MAX'),
        'url': lambda x: 'varchar(256)'
    }

    template_fields = ('s3_key',)

    def __init__(self,
                 sf_conn_id, s3_conn_id, rs_conn_id, # Connection Ids
                 sf_object, # SF Configs
                 rs_schema, rs_table, # RS Configs
                 s3_bucket, s3_key, #S3 Configs
                 *args, **kwargs):

        super().__init__(*args, **kwargs)

        self.sf_conn_id = sf_conn_id
        self.s3_conn_id = s3_conn_id
        self.rs_conn_id = rs_conn_id
        self.sf_object = sf_object
        self.rs_schema = rs_schema
        self.rs_table = rs_table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        super().__init__(*args, **kwargs)

    def fetch_sf_columns(self, sf_conn_id, sf_object):
        """
        Uses Salesforce describe() method to fetch columns from
        Salesforce instance. Compound columns are filtered out.
        """
        sf_conn = SalesforceHook(sf_conn_id).get_conn()

        # Dynamically Fetch the simple_salesforce query method
        # ie. sf_conn.Lead.describe() | sf_conn.Contact.describe()
        sf_fields = sf_conn.__getattr__(sf_object).describe()['fields']

        # Get compound fields
        k1 = 'compoundFieldName'
        compound_fields = [f[k1] for f in sf_fields] # Get all compound fields across all fields
        compound_fields = set(compound_fields)
        compound_fields.remove(None)

        def build_dict(x): return {
            'rs_name': x['name'].lower(),
            'sf_name': x['name'],
            'path': [x['name']],
            'type': x['soapType'].split(':')[-1],
            'length': x['length'],
            'precision': x['precision']
        }

        # Loop through fields and grab columns we want
        return [build_dict(field) for field in sf_fields if field['name'] not in compound_fields]

    def create_tbl_ddl(self, rs_table, rs_schema, sf_cols):
        """
        Creates the Create Table DDL to be executed on the Redshift
        instance. Only run if table does not exist at time of first run.
        """
        ddl = """
            CREATE TABLE
            IF NOT EXISTS {table_schema}.{table_name}
                ({cols});
        """
        def make_col_ddl(sf_col):
            sf_type = sf_col['type']
            type_transform = self.dt_conv[sf_type] # Grab lambda type converter
            rs_type = type_transform(sf_col['length']) # Execute type converter

            return "\t{} {}".format(sf_col['rs_name'], rs_type)

        cols_ddl = [make_col_ddl(col) for col in sf_cols]
        cols_ddl = ', \n'.join(cols_ddl)

        return [ddl.format(table_name=rs_table, table_schema=rs_schema, cols=cols_ddl)]

    def alter_tbl_ddl(self, rs_schema, rs_table, missing_sf_cols):
        """
        Creates the Alter Table DDL that will be executed on the Redshift
        instance. Only run if there is an addtional column to be added.
        """
        alter_ddls = []
        for col in missing_sf_cols:
            sf_type = col['type']
            rs_type = self.dt_conv[sf_type](col['length'])

            alter_ddl = """ALTER TABLE {}.{} ADD COLUMN {} {}"""

            alter_ddls.append(
                alter_ddl.format(
                    rs_schema,
                    rs_table,
                    col['rs_name'].lower(),
                    rs_type
                )
            )

        return alter_ddls

    def fetch_rs_ddl(self, rs_conn_id, rs_table, rs_schema, sf_cols):
        """
        Used to decide whether we need to run an ALTER or CREATE
        table command. Leverages alter_tbl_ddl() and create_tbl_ddl()
        to create the DDL that will be run.
        """
        rs_info = PostgresHook().get_connection(rs_conn_id)
        rs_conn = PostgresHook(rs_conn_id).get_cursor()

        q = """
            SELECT column_name
            FROM information_schema.columns c
            WHERE table_name = '{rs_table}'
            and table_schema = '{rs_schema}'
            and table_catalog = '{rs_database}'
            ORDER BY ordinal_position ASC
        """

        rs_conn.execute(q.format(
            rs_table=rs_table,
            rs_schema=rs_schema,
            rs_database=rs_info.schema
        ))

        rs_cols = rs_conn.fetchall()
        rs_cols = [col[0] for col in rs_cols]
        tbl_missing = False if rs_cols else True

        if tbl_missing:
            ddl = self.create_tbl_ddl(rs_table, rs_schema, sf_cols)

        else:
            # All columns that exist in sf_cols but not in rs_cols
            # missing_cols = {x['name'] for x in sf_cols} - {x[0] for x in rs_cols}
            missing_cols = [x for x in sf_cols if x['rs_name'] not in rs_cols]
            ddl = self.alter_tbl_ddl(rs_schema, rs_table, missing_cols) if missing_cols else None

        rs_conn.close()

        return ddl

    def fetch_rs_columns(self, rs_conn_id, rs_table, rs_schema):
        """
        Fetches the current Redshift columns while maintaining order
        of columns returned. This allows the JSONPath column mapping
        to maintain the same order.
        """
        rs_info = PostgresHook().get_connection(rs_conn_id)
        rs_conn = PostgresHook(rs_conn_id).get_cursor()

        q = """
            SELECT column_name, ordinal_position
            FROM information_schema.columns c
            WHERE table_name = '{rs_table}'
            and table_schema = '{rs_schema}'
            and table_catalog = '{rs_database}'
            ORDER BY ordinal_position ASC
        """.format(rs_table=rs_table, rs_schema=rs_schema, rs_database=rs_info.schema)

        rs_conn.execute(q)
        recs = rs_conn.fetchall()
        rs_conn.close()
        return [rec[0] for rec in recs]

    def create_paths(self, paths):
        """
        Creates JSONPath column mapping string.

        :param paths: Each element of list represents a specific path to a dictionary attribute
                      ex. [
                            ['first_lvl_attr'],
                            ['first_lvl_attr', 'second_lvl_attr']
                        ]
        :type paths: list of lists
        """
        def create_path(field_path):
            """
            """
            base = "\"${}\""
            # Create a template for each value in array
            # reasonable assume iether str or int will be passed
            tmplts = ["['{}']" if isinstance(path, str) else "[{}]" for path in field_path]
            paths = [tmplts[i].format(field_path[i]) for i in range(len(field_path))]
            full_path = ''.join(paths)
            return base.format(full_path)

        base = """"jsonpaths": [ \n\t{}\t]"""
        l = len(paths)
        # List of Template Strings Ready to be Formmatted
        tmplts = ["\t{},\n" for i in range(l)]
        # JSON Paths that will be passted into template strings
        paths = [tmplts[i].format(create_path(paths[i])) for i in range(l)]
        paths[-1] = paths[-1].replace(',', '') # remove first , in path
        paths_str = ''.join(paths) # finally merge into single str

        return "{\n" + base.format(paths_str) + "\n}"

    def generate_path_file(self, rs_cols, sf_cols):
        """
        Takes a list of __ordered__ redshift columns that exist in the dst
        and a list of salesforce columns that exist in the source. Using the
        ordered redshift columns it creates a list of paths to the salesforce
        attributes matching the order of the redshift columns. Returns a
        JSONPath column mapping string matching the order of the
        salesforce columns.

        :param rs_cols: List of tuples, because that is how postgres Hook
                        returns it's results.
        :type rs_cols: list of tuples representing Redshift cols
        :param sf_cols: list of dictionaries representing Salesforce cols
                        'path' and 'rs_name' are required attrs of dict
        :type sf_cols: list of dictionaries
        """
        sf_cols = {sf_col['rs_name']: sf_col['path'] for sf_col in sf_cols}
        ordered_paths = [sf_cols.get(col_key, None) for col_key in rs_cols if sf_cols.get(col_key) is not None]

        return self.create_paths(ordered_paths)

    def build_copy_cmd_template(self, schema, tbl, columns, path_key, path_bucket):
        """
        Builds a partial copy command that can be further templated to avoid storing credentials in xcom
        Notice bucket, key, creds being double wrapped.
        """
        base_copy = """
        COPY {schema}.{tbl} ({columns})
        FROM 's3://{{bucket}}/{{key}}'
        CREDENTIALS '{{creds}}'
        JSON 's3://{path_bucket}/{path}'
        REGION as 'us-east-1'
        TIMEFORMAT 'epochmillisecs'
        TRUNCATECOLUMNS
        COMPUPDATE OFF
        STATUPDATE OFF;
        """

        return base_copy.format(
            schema=schema,
            tbl=tbl,
            columns=columns,
            path=path_key,
            path_bucket=path_bucket
        )
    
    def execute(self, context):
        """
        See class definition.
        """
        # Get Columns From Salesforce
        sf_cols = self.fetch_sf_columns(self.sf_conn_id, self.sf_object)
        # Check if we need DDL changes
        rs_ddl = self.fetch_rs_ddl(self.rs_conn_id, self.rs_table, self.rs_schema, sf_cols)

        # Run RS DDL Changes
        if rs_ddl is not None:
            # return rs_ddl
            rs = PostgresHook(self.rs_conn_id)
            for ddl in rs_ddl:
                rs.run(ddl)

        # Get Columns From Redshift
        rs_cols = self.fetch_rs_columns(self.rs_conn_id, self.rs_table, self.rs_schema)
        
        # Generate JSONPath String w/ same ordering as RS Table Columns
        jsonPath = self.generate_path_file(rs_cols, sf_cols)

        # Push JSONPath to S3
        s3 = S3Hook(self.s3_conn_id)
        s3.load_string(jsonPath, self.s3_key, bucket_name=self.s3_bucket, replace=True)

        # Limit Columns in Copy CMD to only Columns in SF
        filter_list = [col['rs_name'] for col in sf_cols]
        filtered_cols = [col for col in rs_cols if col in filter_list]

        rs_col_str = ', '.join(filtered_cols)

        copy_cmd = self.build_copy_cmd_template(
            schema=self.rs_schema,
            tbl=self.rs_table,
            columns=rs_col_str,
            path_key=self.s3_key,
            path_bucket=self.s3_bucket
        )

        self.xcom_push(context, key='copy_cmd', value=copy_cmd)

        # Push SF Columns to Xcom or S3
        self.xcom_push(context, key='sf_cols', value=[col['sf_name'] for col in sf_cols])