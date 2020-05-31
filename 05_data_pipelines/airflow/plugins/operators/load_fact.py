from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                redshift_conn_id = "",
                table="",
                sql="",
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.table = table
        self.sql = sql
    def execute(self, context):
        self.log.info('Loading fact table')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        self.log.info("Clearing data from fact table")
        redshift.run("DELETE FROM {};".format(self.table))
        
        # self.log.info("Deleting temp fact table")
        # redshift.run("DROP TABLE IF EXISTS temp_songplays")

        # sql = """
        #     CREATE TABLE temp_songplays AS
        #     SELECT
        #     DISTINCT TIMESTAMP 'epoch' + ts::INT8/1000 * INTERVAL '1 second' start_time,
        #     a.userid, a.level, b.song_id, b.artist_id, a.sessionid, a.location, a.userAgent
        #     from staging_events a
        #     inner join staging_songs b on a.artist = b.artist_name and a.song = b.title
        #     WHERE a.page = 'NextSong'

        # """
        self.log.info("Creating temp fact table")
        redshift.run(self.sql)
        # sql = """
        # INSERT INTO songplays(start_time, userid, level, songid, artistid, 
        # sessionid, location, user_agent)
        # SELECT
        #     start_time, userid, level, song_id, artist_id, sessionid, location, useragent
        # FROM (
        # SELECT
        #     ROW_NUMBER() OVER (PARTITION BY start_time order by start_time) AS r,
        #     t.*
        # from temp_songplays t
        # ) x
        # where x.r = 1
        # """
        # self.log.info("Creating fact table")
        # redshift.run(sql)