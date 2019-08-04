from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 redshift_database_schema="",
                 dim_table="",
                 sql_query="",
                 truncate_before_insert=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_database_schema=redshift_database_schema
        self.dim_table = dim_table
        self.sql_query = sql_query
        self.truncate_before_insert = truncate_before_insert

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_before_insert:
            self.log.info("Truncating table {} before insert ".format(self.dim_table))
            redshift_hook.run('DELETE FROM {}'.format(self.dim_table))
        
        self.log.info("Inserting records into table {} ".format(self.dim_table))
        insert_query = 'INSERT INTO {}.{} ({})'.format(self.redshift_database_schema, self.dim_table, self.sql_query)
        redshift_hook.run(insert_query)
