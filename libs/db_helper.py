import pymysql
import time
from libs.log import Log

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

    def _get_cdate(self, x_day_before, today=None):
        if not today:
            today = time.time()
        day_seconds = 60 * 60 * 24
        target_timestamp = today - day_seconds * x_day_before
        target_date = time.localtime(target_timestamp)
        year = target_date.tm_year
        if target_date.tm_mon < 10:
            month = '0%s' % target_date.tm_mon
        else:
            month = target_date.tm_mon

        if target_date.tm_mday < 10:
            day = '0%s' % target_date.tm_mday
        else:
            day = target_date.tm_mday

        return '%s%s%s' % (year, month, day), '%s-%s-%s' % (year, month, day)

    def get_latest_version_data(self, app):
        db_koala = self._connect_koala_database()
        cursor_koala = self._get_cursor(db_koala)
        _, formated_cdate = self._get_cdate(2)
        data = self._run_sql_and_commit(db_koala, cursor_koala, app.get_version_sql(formated_cdate))
        return data
