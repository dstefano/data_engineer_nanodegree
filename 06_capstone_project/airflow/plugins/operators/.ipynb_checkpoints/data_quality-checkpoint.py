from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                tables="",
                *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            #print("Table {}").format(table)
            sql = "SELECT count(*) from {}".format(table)
            rows = redshift.get_records(sql)

            if len(rows) == 0:
                raise ValueError("Data quality check: table is empty")
            else:
                print("Table is OK.")