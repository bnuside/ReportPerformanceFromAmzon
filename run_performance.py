from libs.db_helper import Database
from libs.apps import Kika2019
from libs.log import Log
from libs.common import get_cdate

def make_data(app, database):
    log = Log()
    version_data = database.get_latest_version_data(app)
    cdate, _ = get_cdate(2)
    # try:
    version_data = ['\'%s\'' % v[0] for v in version_data]
    version_data = ','.join(version_data)
    version_data = '(%s)' % version_data
    print(version_data)

    matrix_data = database.get_matrix_data(app, cdate, version_data)
    print(len(matrix_data.splitlines()[3].strip().split('\t')))
    # except Exception as e:
    #     log.logger.info(e)


def debug():
    v = (('6.6.9.4456',), ('6.6.9.4446',), ('6.6.9.4432',), ('6.6.9.4420',), ('6.6.9.4406',))
    v = ['"%s"'%vv[0] for vv in v]
    print(','.join(v))


if __name__ == '__main__':
    kika2019 = Kika2019()
    database = Database()
    data_for_insert = make_data(kika2019, database)