from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self, redshift_conn_id="redshift", *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        
        self.redshift_conn_id = redshift_conn_id
        self.target_table = kwargs['params']['target_table']
        self.s3_bucket = kwargs['params']['s3_bucket']
        self.aws_credentials = kwargs['params']['aws_credentials']
        self.format = kwargs['params']['format']
        

    def execute(self, context):
        self.log.info(f'redshift conn id  {self.redshift_conn_id}')
        self.log.info(f'aws_credentials   {self.aws_credentials}')
        self.log.info(f'target_table      {self.target_table}')
        self.log.info(f's3_bucket         {self.s3_bucket}')
        self.log.info(f'format         {self.format}')
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        copy_statement = ''
        
        if self.format == 'json':
            copy_statement = f"COPY {self.target_table} FROM '{self.s3_bucket}' access_key_id '{self.aws_credentials.get('AWS_KEY')}' secret_access_key '{self.aws_credentials.get('AWS_SECRET')}' json 'auto' region 'us-west-2' timeformat 'YYYY-MM-DD HH:MI:SS'"          
        else:
            copy_statement = f"COPY {self.target_table} FROM '{self.s3_bucket}' ignoreheader 1 access_key_id '{self.aws_credentials.get('AWS_KEY')}' secret_access_key '{self.aws_credentials.get('AWS_SECRET')}' csv region 'us-west-2' delimiter ';'"
            
        
        redshift_hook.run(copy_statement)
        