
class App():
    def __init__(self):
        pass

    def get_version_sql(self, formated_cdate):
        """

        :param formated_cdate: 类似「2019-05-28」这样格式的日期
        :return:
        """

        return "select g_key,g_value from app_details_dlu where create_date='{cdate}' and app_key='{appkey}' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 5;".format(cdate=formated_cdate, appkey=self.app_key)

    def get_performance_data_sql(self, formated_cdate, formated_versions):
        return "select cdate,app_key,version,nation, avg(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end) as app_create_time, avg(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end) as kb_create_1st_time, avg(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end) as kb_createview_startup_time, avg(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end) as kb_createview_startup_1st_time, avg(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end) as kb_warm_startup_time, avg(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end) as emoji_time, avg(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end) as menu_time, avg(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end) as suggestions_time, avg(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end) as slide_suggestions_time, approx_percentile(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end,0.95) as app_create_time_95, approx_percentile(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end,0.95) as kb_create_1st_time_95, approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end,0.95) as kb_createview_startup_time_95, approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end,0.95) as kb_createview_startup_1st_time_95, approx_percentile(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end,0.95) as kb_warm_startup_time_95, approx_percentile(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end,0.95) as emoji_time_95, approx_percentile(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end,0.95) as menu_time_95, approx_percentile(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end,0.95) as suggestions_time_95, approx_percentile(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end,0.95) as slide_suggestions_time_95  from gimbal.event where app_key='{appkey}' and layout='performance' and item='key_metrics' and cdate={cdate} and event_part='other'  and version in {ver} and nation in ('us','br','id','mx','ph','ru','ar','co','fr','es') group by cdate,app_key,version,nation".format(appkey=self.appkey, cdate=formated_cdate, ver=formated_versions)

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