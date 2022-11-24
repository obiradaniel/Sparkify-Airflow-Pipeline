from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query=sql_query

    def execute(self, context):
        #self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Inserting Fact data into {}.'.format(self.table))
        sql_to_run =  '''INSERT INTO {} {}'''.format(self.table, self.sql_query)
        redshift.run(sql_to_run)
        CountRecords="""Select COUNT(*) from {}""".format(self.table)
        Records = redshift.get_first(CountRecords)[0]
        self.log.info("{}, Records loaded to {} table from staging.".format(Records, self.table))
