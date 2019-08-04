from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                 redshift_database_schema="",
                 fact_table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.redshift_database_schema=redshift_database_schema
        self.fact_table = fact_table
        self.sql_query = sql_query

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Checking if songplays table exists on Redshift")
        redshift_hook.run("""
            CREATE TABLE IF NOT EXISTS public.songplays (
            	playid varchar(32) NOT NULL,
            	start_time timestamp NOT NULL,
            	userid int4 NOT NULL,
            	"level" varchar(256),
            	songid varchar(256),
            	artistid varchar(256),
            	sessionid int4,
            	location varchar(256),
            	user_agent varchar(256),
            	CONSTRAINT songplays_pkey PRIMARY KEY (playid)
            );
        """)
        self.log.info('Loading data fact tables using LoadFactOperator')
        insert_query = 'INSERT INTO {}.{} ({})'.format(self.redshift_database_schema, self.fact_table, self.sql_query)
        redshift_hook.run(insert_query)
