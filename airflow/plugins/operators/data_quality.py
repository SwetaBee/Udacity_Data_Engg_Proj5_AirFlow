from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Run data quality checks on one or more tables.
    
    Parameters:
    data_check_query: A list of one or more queries to check data.
    table: A list of one or more tables for the data check queries.
    expected_results: A list of expected results for each data check query.
    
    Returns: Exception raised on data check failure.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 check_stmts=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.check_stmts = check_stmts

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")

        for stmt in self.check_stmts:
            result = int(redshift_hook.get_first(sql=stmt['sql'])[0])

            # check if equal
            if stmt['op'] == 'eq':
                if result != stmt['val']:
                    raise AssertionError(f"Data Quality Check failed: {result} {stmt['op']} {stmt['val']}")
            # check if not equal
            elif stmt['op'] == 'ne':
                if result == stmt['val']:
                    raise AssertionError(f"Data Quality Check failed: {result} {stmt['op']} {stmt['val']}")
            # check if greater than
            elif stmt['op'] == 'gt':
                if result <= stmt['val']:
                    raise AssertionError(f"Data Quality Check failed: {result} {stmt['op']} {stmt['val']}")

            self.log.info(f"Data Quality Check Passed: {result} {stmt['op']} {stmt['val']}")
