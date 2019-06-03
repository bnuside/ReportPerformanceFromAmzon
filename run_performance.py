from libs.db_helper import Database
from libs.apps import Kika2019
from libs.log import Log
from libs.common import get_cdate

def make_data():
    log = Log()
    kika2019 = Kika2019()
    database = Database()
    version_data = database.get_latest_version_data(kika2019)
    cdate, _ = get_cdate(2)
    # try:
    version_data = ['"%s"' % v[0] for v in version_data]
    version_data = ','.join(version_data)
    version_data = '(%s)' % version_data
    print(version_data)

    matrix_data = database.get_matrix_data(kika2019, cdate, version_data)
    print(matrix_data)
    # except Exception as e:
    #     log.logger.info(e)


def debug():
    v = (('6.6.9.4456',), ('6.6.9.4446',), ('6.6.9.4432',), ('6.6.9.4420',), ('6.6.9.4406',))
    v = ['"%s"'%vv[0] for vv in v]
    print(','.join(v))


if __name__ == '__main__':
    make_data()