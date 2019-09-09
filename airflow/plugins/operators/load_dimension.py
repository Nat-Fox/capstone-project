from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_table = kwargs['params']['target_table']
        self.sql = kwargs['params']['sql']
        self.append_data = kwargs['params']['append_data']
        self.insert_fields = kwargs['params']['insert_fields']
        
    def execute(self, context):
        self.log.info(f'redshift conn id  {self.redshift_conn_id}')
        self.log.info(f'target_table      {self.target_table}')
        self.log.info(f'sql  {self.sql}')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        
        if self.append_data == True:
            redshift_hook.run(f"INSERT INTO {self.target_table} ({','.join(self.insert_fields)}) {self.sql}")
        else:            
            redshift_hook.run(f'TRUNCATE table {self.target_table}')
            redshift_hook.run(f"INSERT INTO {self.target_table} ({','.join(self.insert_fields)}) {self.sql}")
            
            