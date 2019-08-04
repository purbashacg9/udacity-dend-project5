from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_quality_checks = data_quality_checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for data_quality_check in self.data_quality_checks:
            query = data_quality_check['sql_test']
            expected_result = data_quality_check['expected_result']
            self.log.info(f"Running check {query} with expected result {expected_result}")
            test_result = redshift_hook.get_records(query)
            if test_result[0][0] != expected_result:
                raise ValueError(f"Data quality check failed, test result {test_result}")
            else:
                self.log.info(f"Data Quality check passed, test Result {test_result}.")
