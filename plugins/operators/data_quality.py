from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.sql import (
    SQLCheckOperator,
    SQLValueCheckOperator,
    SQLIntervalCheckOperator,
    SQLThresholdCheckOperator
)

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_tests= [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_tests=sql_tests


    def execute(self, context):
        #self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for i, test in enumerate(self.sql_tests):
            keys = list(test.keys())
            query = test[keys[0]] # Query should have a one line result
            value = test[keys[1]]
            red_shift_value = redshift.get_first(query)[0]
            test_no = i + 1 # enumerate starts from 0, thus the ith test will 
            #be i+1
            
            if value == red_shift_value:
                self.log.info("SQLTest {}: {} expecting {}, got {}: PASSED.".format(test_no, query, value, red_shift_value))
            else:
                self.log.info("SQLTest {}: {} expecting {}, got {}: FAILED.".format(test_no, query, value, red_shift_value))
                raise ValueError("SQLTest {}: {} expected {}, got {}: FAILED.".format(test_no, query, value, red_shift_value))
            
        if len(self.sql_tests) == 0:
            self.log.info("NO SQL Tests Provided")
        else:
            self.log.info("All {} SQL Tests Passed.".format(len(self.sql_tests)))
                