from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key", )
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON  '{}'
    """

    @apply_defaults
    def __init__(self,
        redshift_conn_id="",
        aws_credentials_id="",
        s3_bucket="",
        s3_key="",
        table="",
        json_load_option="",
        *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.json_load_option = json_load_option


    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_key_rendered = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, s3_key_rendered)
        self.log.info('Reading files from staging location {}'.format(s3_path))

        sql_stmt = self.copy_sql.format(
            self.table,
    	    s3_path,
    	    credentials.access_key,
            credentials.secret_key,
            self.json_load_option
        )
        redshift_hook.run(sql_stmt)
