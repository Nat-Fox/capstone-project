from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.target_table = kwargs['params']['target_table']
        self.sql = kwargs['params']['sql']
        self.insert_fields = kwargs['params']['insert_fields']

    def execute(self, context):
        
        self.log.info(f'redshift conn id  {self.redshift_conn_id}')
        self.log.info(f'target_table      {self.target_table}')
        self.log.info(f'sql  {self.sql}')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(f"INSERT INTO {self.target_table} ({','.join(self.insert_fields)}) {self.sql}")
        
        
