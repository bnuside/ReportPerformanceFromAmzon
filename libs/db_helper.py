import pymysql
import os
from libs.log import Log
from libs.env import Env
from libs.common import get_cdate

class Database(object):
    def __init__(self):
        self._db_host = '172.31.21.163'
        self._db_user = 'kikatechuser'
        self._db_pass = 'r9ca#J40cd39'

        self._db_name_koala = 'koala'
        self._db_name_matrix = 'kika_key_metrics'
        self._log = Log()
        self._db_koala = None
        self._db_matrix = None
        self._env = Env()

    def _connect_database_server(self, db_name):
        try:
            db = pymysql.connect(host=self._db_host, user=self._db_user, passwd=self._db_pass, db=db_name)
            return db
        except Exception as e:
            self._log.logger.error(e)
            try:
                db.close()
            except Exception as e2:
                self._log.logger.error(e2)

    def _connect_koala_database(self):
        if not self._db_koala:
            self._db_koala = self._connect_database_server(self._db_name_koala)
        return self._db_koala

    def _connect_matrix_database(self):
        if not self._db_matrix:
            self._db_matrix = self._connect_database_server(self._db_name_matrix)
        return self._db_matrix

    def _get_cursor(self, db):
        if not db:
            return

        return db.cursor()

    def _run_sql_and_commit(self, db, cursor, sql):
        if not cursor or not db:
            return
        try:
            cursor.execute(sql)
            db.commit()
            result = cursor.fetchall()
            return result
        except Exception as e:
            self._log.logger.error(e)

    def get_latest_version_data(self, app):
        db_koala = self._connect_koala_database()
        cursor_koala = self._get_cursor(db_koala)
        _, formated_cdate = get_cdate(2)
        data = self._run_sql_and_commit(db_koala, cursor_koala, self._get_version_sql(app, formated_cdate))
        return data

    def get_matrix_data(self, app, formated_cdate, formated_versions):
        sql = self._get_performance_data_sql(app, formated_cdate, formated_versions)
        return self.execute_athena_sql(sql)

    def _get_version_sql(self, app, formated_cdate):
        """

        :param formated_cdate: 类似「2019-05-28」这样格式的日期
        :return:
        """

        return "select g_key from app_details_dlu where create_date='{cdate}' and app_key='{appkey}' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 5;".format(
            cdate=formated_cdate, appkey=app.app_key)

    def _get_performance_data_sql(self, app, cdate, formated_versions):
        return "select cdate,app_key,version,nation, avg(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end) as app_create_time, avg(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end) as kb_create_1st_time, avg(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end) as kb_createview_startup_time, avg(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end) as kb_createview_startup_1st_time, avg(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end) as kb_warm_startup_time, avg(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end) as emoji_time, avg(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end) as menu_time, avg(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end) as suggestions_time, avg(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end) as slide_suggestions_time, approx_percentile(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end,0.95) as app_create_time_95, approx_percentile(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end,0.95) as kb_create_1st_time_95, approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end,0.95) as kb_createview_startup_time_95, approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end,0.95) as kb_createview_startup_1st_time_95, approx_percentile(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end,0.95) as kb_warm_startup_time_95, approx_percentile(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end,0.95) as emoji_time_95, approx_percentile(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end,0.95) as menu_time_95, approx_percentile(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end,0.95) as suggestions_time_95, approx_percentile(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end,0.95) as slide_suggestions_time_95  from gimbal.event where app_key='{appkey}' and layout='performance' and item='key_metrics' and cdate={cdate} and event_part='other'  and version in {ver} and nation in ('us','br','id','mx','ph','ru','ar','co','fr','es') group by cdate,app_key,version,nation".format(appkey=app.app_key, cdate=cdate, ver=formated_versions)

    def execute_athena_sql(self, sql):
        cmd = '{athena_executor} "{s}"'.format(athena_executor=self._env.athena_executor, s=sql)
        raw = os.popen(cmd)
        ret = raw.read()
        raw.close()
        return ret