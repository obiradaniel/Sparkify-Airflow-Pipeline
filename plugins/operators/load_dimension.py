from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_query="",
                 overwrite=False,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.sql_query=sql_query
        self.overwrite=overwrite

    def execute(self, context):
        #self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.overwrite:
            CountRecords="""Select COUNT(*) from {}""".format(self.table)
            Records = redshift.get_first(CountRecords)[0]
            self.log.info("{}, Records to be Deleted from {} table.".format(Records, self.table))
            sql_run =  '''DELETE FROM {}'''.format(self.table)
            redshift.run(sql_run)
            self.log.info('Inserting Dimension data into {}.'.format(self.table))
            sql_run =  '''INSERT INTO {} {}'''.format(self.table, self.sql_query)
            redshift.run(sql_run)
            CountRecords="""Select COUNT(*) from {}""".format(self.table)
            Records = redshift.get_first(CountRecords)[0]
            self.log.info("{}, Records loaded to {} table from staging.".format(Records, self.table))
        else:
            self.log.info('Inserting Dimension data into {}.'.format(self.table))
            sql_run =  '''INSERT INTO {} {} {}'''.format(self.table, self.sql_query)
            redshift.run(sql_run)
            CountRecords="""Select COUNT(*) from {}""".format(self.table)
            Records = redshift.get_first(CountRecords)[0]
            self.log.info("{}, Records loaded to {} table from staging.".format(Records, self.table))
