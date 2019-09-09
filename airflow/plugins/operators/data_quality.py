from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_checks = kwargs['params']['sql_checks']
        

    def execute(self, context):
        self.log.info(f'redshift conn id  {self.redshift_conn_id}')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        failing_tests = []
        error_count = 0
        
        for check in self.sql_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected')
            op = check.get('operation')
            
            records = redshift_hook.get_records(sql)[0]
            
            if op == 'eq':                
                if exp_result != records[0]:
                    error_count += 1
                    failing_tests.append(sql)
            if op == 'gt':
                if records[0] <= exp_result:
                    error_count += 1
                    failing_tests.append(sql)
                

        if error_count > 0:
            raise ValueError(f'Errors {failing_tests}')
