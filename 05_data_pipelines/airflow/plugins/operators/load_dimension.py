from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                redshift_conn_id="",
                table="",                 
                sql="",
                delete_load="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.sql = sql
        self.table = table
        self.delete_load = delete_load

    def execute(self, context):
        self.log.info('Loading fact table table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if delete_load:
            redshift.run("DELETE FROM {};".format(self.table))

        redshift.run(self.sql)