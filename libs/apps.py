
class App():
    def __init__(self):
        pass

    def get_version_sql(self, formated_cdate):
        """

        :param formated_cdate: 类似「2019-05-28」这样格式的日期
        :return:
        """

        # return "select g_key,g_value from app_details_dlu where create_date='{cdate}' and app_key='{appkey}' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 5;".format(cdate=formated_cdate, appkey=self.app_key)
        return "select * from app_details_dlu where create_date='{cdate}' and app_key='{appkey}' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 5;".format(cdate=formated_cdate, appkey=self.app_key)


class Kika(App):
    def __init__(self):
        App.__init__(self)

    @property
    def app_key(self):
        return '78472ddd7528bcacc15725a16aeec190'

class Pro(App):
    def __init__(self):
        App.__init__(self)

    @property
    def app_key(self):
        return '4e5ab3a6d2140457e0423a28a094b1fd'

class iKey(App):
    def __init__(self):
        App.__init__(self)

    @property
    def app_key(self):
        return 'e2934742f9d3b8ef2b59806a041ab389'

class Kika2019(App):
    def __init__(self):
        App.__init__(self)

    @property
    def app_key(self):
        return '73750b399064a5eb43afc338cd5cad25'


if __name__ == '__main__':
    kika = Kika2019()
    print(kika.get_version_sql('03453'))