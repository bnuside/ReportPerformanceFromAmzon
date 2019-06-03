from libs.db_helper import Database
from libs.apps import Kika2019

def make_data():
    kika2019 = Kika2019()
    database = Database()
    version_data = database.get_latest_version_data(kika2019)
    print(version_data)


if __name__ == '__main__':
    make_data()