from libs.db_helper import Database
from libs.apps import Kika2019
from libs.apps import Pro
from libs.apps import iKey
from libs.apps import Kika
from libs.log import Log
from libs.common import get_cdate


log = Log()

def debug():
    v = (('6.6.9.4456',), ('6.6.9.4446',), ('6.6.9.4432',), ('6.6.9.4420',), ('6.6.9.4406',))
    v = ['"%s"' % vv[0] for vv in v]
    print(','.join(v))


if __name__ == '__main__':
    kika2019 = Kika2019()
    pro = Pro()
    ikey = iKey()
    kika = Kika()
    apps = [kika2019, pro, ikey, kika]
    cdate, formatted_cdate = get_cdate(2)
    database = Database()
    for app in apps:
        log.logger.info('Processing matrix data of %s.' % app.name)
        data_for_insert = database.get_matrix_event_data(app,  cdate, formatted_cdate, limit_version=True, limit_nation=True)
        database.insert_data_to_matrix_db(kika2019, data_for_insert, 5)

        # data_for_insert = database.get_matrix_event_data(app,  cdate, formatted_cdate, limit_version=True, limit_nation=False)
        # database.insert_data_to_matrix_db(kika2019, data_for_insert, 5)
        #
        # data_for_insert = database.get_matrix_event_data(app,  cdate, formatted_cdate, limit_version=False, limit_nation=False)
        # database.insert_data_to_matrix_db(kika2019, data_for_insert, 5)

