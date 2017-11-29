from airflow.models import BaseOperator

class S3ToRedshiftOperator(BaseOperator):
    """
    S3 -> Redshift via COPY Commands
    """

    template_fields = ('s3_key','copy_cmd')

    def __init__(self,
                 s3_conn_id, s3_bucket, s3_key,
                 rs_conn_id, rs_schema, rs_table,
                 copy_cmd, load_type='append',
                 join_key=None, incremental_key=None,
                 *args, **kwargs):

        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

        self.rs_conn_id = rs_conn_id
        self.rs_schema = rs_schema
        self.rs_table = rs_table

        self.copy_cmd = copy_cmd
        self.load_type = load_type
        self.join_key = join_key
        self.incremental_key = incremental_key

        # Used In Case of Upsert
        self.tmp_tbl = None
        self.tmp_schema = None

        if self.load_type not in ["append", "upsert"]:
            raise Exception('Please choose "append", "rebuild", or "upsert".')

        if self.load_type == 'upsert' and (self.join_key is None or self.incremental_key is None):
            raise Exception('Upserts require join_key and incremental_key to be specified')

    def drop_tbl_ddl(self, schema, tbl, if_exists=True):
        base_drop = "DROP TABLE {if_exists} {schema}.{tbl}"

        if_exists = 'if exists' if if_exists else ''

        return base_drop.format(
            if_exists=if_exists,
            schema=schema,
            tbl=tbl
        )

    def duplicate_tbl_schema(self, old_schema, old_tbl, new_tbl=None, new_schema=None):
        new_tbl = new_tbl if new_tbl is not None else old_tbl
        new_schema = new_schema if new_schema is not None else old_schema

        cmd = 'CREATE TABLE {new_schema}.{new_tbl}(LIKE {old_schema}.{old_tbl});'

        # give new_tbl a unique name in case of more than one task running
        rand4 = ''.join((choice(ascii_lowercase) for i in range(4)))
        new_tbl += '_tmp_' + rand4 if new_tbl==old_tbl else ''

        self.tmp_tbl = new_tbl
        self.tmp_schema = new_schema

        return cmd.format(
            new_schema=new_schema,
            new_tbl=new_tbl,
            old_schema=old_schema,
            old_tbl=old_tbl
        )

    def del_from_tbl_ddl(self, del_schema, del_tbl, join_schema, join_tbl, conditions=None):
        delete = """DELETE FROM {src_schema}.{src_tbl} USING {join_schema}.{join_tbl} join_tbl"""

        delete = delete.format(
            src_schema=del_schema,
            src_tbl=del_tbl,
            join_schema=join_schema,
            join_tbl=join_tbl
        )

        if conditions:
            delete += '\nWHERE '
            delete += '\nAND '.join(conditions)

        return delete

    def insert_stg_into_dst_ddl(self, dst_schema, dst_tbl, stg_schema, stg_tbl):
        insert = """insert into {dst_schema}.{dst_tbl}\n (select * from {stg_schema}.{stg_tbl});"""

        return insert.format(
            dst_schema=dst_schema,
            dst_tbl=dst_tbl,
            stg_schema=stg_schema,
            stg_tbl=stg_tbl
        )

    def execute(self, context):
        """
        Runs copy command on redshift
        """
        pg = PostgresHook(postgres_conn_id=self.rs_conn_id)

        a_key, s_key = S3Hook(s3_conn_id=self.s3_conn_id).get_credentials()
        conn_str = 'aws_access_key_id={};aws_secret_access_key={}'.format(a_key, s_key)

        # If append -> normal copy into table
        if self.load_type == 'append':
            copy_cmd = self.copy_cmd.format(creds=conn_str, bucket=self.s3_bucket, key=self.s3_key)
            pg.run(copy_cmd)

        else:
            # Duplicate Dst Tbl
            duplicate_tbl = self.duplicate_tbl_schema(self.rs_schema, self.rs_table)
            pg.run(duplicate_tbl)

            copy_cmd = self.copy_cmd.format(creds=conn_str, bucket=self.s3_bucket, key=self.s3_key)
            pg.run(copy_cmd)

            # DELETE Duplicate Rows
            del_conditions = [
                "{}.{} = join_tbl.{}".format(
                    self.rs_table,
                    self.join_key,
                    self.join_key
                ),
                "{}.{} < join_tbl.{}".format(
                    self.rs_table,
                    self.incremental_key,
                    self.incremental_key
                )
            ]

            del_ddl = self.del_from_tbl_ddl(
                self.rs_schema,
                self.rs_table, 
                self.tmp_schema,
                self.tmp_tbl,
                del_conditions,
            )
            pg.run(del_ddl)

            # Do Inserts
            insert_ddl = self.insert_stg_into_dst_ddl(
                self.rs_schema,
                self.rs_table,
                self.tmp_schema,
                self.tmp_tbl,
            )
            pg.run(insert_ddl)

            # Cleanup Temp Table
            drop_ddl = self.drop_tbl_ddl(self.tmp_schema, self.tmp_tbl)
            pg.run(drop_ddl)