#!/usr/bin/python
# -*- coding: UTF-8 -*-

import subprocess
import pymysql
import time
import datetime

athena_shell = "/usr/local/jdk1.8.0_121/bin/java -classpath libs/athena_query_kika_tool-1.0.jar:" \
               "libs/AthenaJDBC41-1.0.0.jar:" \
               "libs/mysql-connector-java-5.1.40-bin.jar" \
               " com.kika.tech.athena_query_kika_tool.AthenaQueryTool" \
               " AKIAIUO2VW53QUXXMCFQ "


def get_cdate(i):
    time_today = time.localtime(time.time())
    year = time_today.tm_year
    mon = time_today.tm_mon
    day = time_today.tm_mday
    return str(datetime.datetime(year, mon, day) - datetime.timedelta(i)).split()[0].replace('-', '')


def test_insert_top5_and_nations():
    cdate_2 = get_cdate(2)
    cdate_2_format = cdate_2[0:4] + '-' + cdate_2[4:6] + '-' + cdate_2[6:]

    # athena_shell = "/usr/local/jdk1.8.0_51/bin/java -classpath /home/pubsrv/project/athenaaccess/libs/athena_query_kika_tool-1.0.jar:" \
    #                "/home/pubsrv/project/athenaaccess/libs/AthenaJDBC41-1.0.0.jar:" \
    #                "/home/pubsrv/project/athenaaccess/libs/mysql-connector-java-5.1.40-bin.jar" \
    #                " com.kika.tech.athena_query_kika_tool.AthenaQueryTool" \
    #                " AKIAIUO2VW53QUXXMCFQ "

    """
    1. 定义top10国家
       'us','br','id','mx','ph','ru','ar','co','fr','es'

    2. 定义top5版本
       172.31.21.163
       koala
       app_details_dlu

       kika top5版本
       select g_key,g_value from app_details_dlu where create_date='2017-06-21' and app_key='78472ddd7528bcacc15725a16aeec190' and gby_type='appversion' and g_value>1000 order by g_key desc limit 5;
       pro top5
       select g_key,g_value from app_details_dlu where create_date='2017-06-21' and app_key='4e5ab3a6d2140457e0423a28a094b1fd' and gby_type='appversion' and g_value>1000 order by g_key desc limit 5;
       ikey top5
       select g_key,g_value from app_details_dlu where create_date='2017-06-21' and app_key='e2934742f9d3b8ef2b59806a041ab389' and gby_type='appversion' and g_value>1000 order by g_key desc limit 5;

    3.分APPKEY查询

      select cdate,app_key,version,nation,
      approx_percentile(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' then cast( json_extract_scalar(extra,'$.app_create') as INTEGER) end) as app_create_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_create_1st') as INTEGER) end) as kb_create_1st_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup') as INTEGER) end) as kb_createview_startup_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as INTEGER) end) as kb_createview_startup_1st_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_warm_startup') as INTEGER) end) as kb_warm_startup_time,
      approx_percentile(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' then cast( json_extract_scalar(extra,'$.emoji_time') as INTEGER) end) as emoji_time,
      approx_percentile(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' then cast( json_extract_scalar(extra,'$.menu_time') as INTEGER) end) as emoji_time,
      approx_percentile(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.suggestions_time') as INTEGER) end) as suggestions_time,
      approx_percentile(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as INTEGER) end) as slide_suggestions_time from event where app_key='XXXXXXX' and cdate=XXXXXXXXX  and event_part='other'  and version in ('XX','XX','XX') and nation in ('us','br','id','mx','ph','ru','ar','co','fr','es') group by cdate,app_key,version,nation



    Keyboard_Tech

    有两种方式将指标入库
    1.    程序
           MySQL地址为：
           AWS内网IP：172.31.21.163
           db_nme: kika_key_metric
           TableName: keyboard_tech
           user:  kikatechuser
           pwd:  r9ca#J40cd39

           可读可写可删可以更改表结构

    2.    Web访问   http://phpadmin.kika-backend.com/
           用户名密码同上，登陆后选择数据库和表，在Insert处可以插入数据。(以下表名只是示范，每个账号只能看到自己的表）
           Inline image 1

    """
    k1 = '\'78472ddd7528bcacc15725a16aeec190\''
    k2 = '\'4e5ab3a6d2140457e0423a28a094b1fd\''
    k3 = '\'e2934742f9d3b8ef2b59806a041ab389\''
    k4 = '\'b1f6dd09ec315aa442bbb01d0663dd22\''
    k5 = "\'73750b399064a5eb43afc338cd5cad25\'"

    sql_list = [
        # k3
        # dnu_organic
    ]

    db_kola = pymysql.connect(host="172.31.21.163", user="kikatechuser", passwd="r9ca#J40cd39", db="koala")

    db = pymysql.connect(host="172.31.21.163", user="kikatechuser", passwd="r9ca#J40cd39", db="kika_key_metrics")

    select_version = {
        # kika top5版本
        "78472ddd7528bcacc15725a16aeec190": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='78472ddd7528bcacc15725a16aeec190' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 5;",
        # pro top5
        "4e5ab3a6d2140457e0423a28a094b1fd": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='4e5ab3a6d2140457e0423a28a094b1fd' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 5;",
        # ikey top5
        "e2934742f9d3b8ef2b59806a041ab389": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='e2934742f9d3b8ef2b59806a041ab389' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 5;",
        # kikaindic top5
        "b1f6dd09ec315aa442bbb01d0663dd22": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='b1f6dd09ec315aa442bbb01d0663dd22' and gby_type='appversion' and g_value>1000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 5;",
        # clavier top5
        "73750b399064a5eb43afc338cd5cad25": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='73750b399064a5eb43afc338cd5cad25' and gby_type='appversion' and g_value>1000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 5;"

    }

    cursor = db.cursor()
    cursor_kola = db_kola.cursor()

    """
    create_date	app_key	version	nation	app_create_time	kb_create_1st_time	kb_createview_startup_time	kb_createview_startup_1st_time	kb_warm_startup_time	emoji_time	menu_time	suggestions_time	slide_suggestions_time
    """
    insert_keys = ["create_date", "app_key", "version", "nation", "app_create_time", "kb_create_1st_time",
                   "kb_createview_startup_time",
                   "kb_createview_startup_1st_time", "kb_warm_startup_time", "emoji_time", "menu_time",
                   "suggestions_time", "slide_suggestions_time",
                   "app_create_time_95", "kb_create_1st_time_95",
                   "kb_createview_startup_time_95", "kb_createview_startup_1st_time_95", "kb_warm_startup_time_95",
                   "emoji_time_95", "menu_time_95", "suggestions_time_95",
                   "slide_suggestions_time_95"]
    count_key = len(insert_keys)
    for appkey, sel_ver in select_version.items():
        try:
            print(sel_ver)
            cursor_kola.execute(sel_ver)
            db_kola.commit()
            result = cursor_kola.fetchall()
        except:
            continue
        verson_str = "("
        for g_key, g_value in result:
            verson_str += "'" + str(g_key) + "',"
        verson_str = verson_str[:-1] + ")"
        print(verson_str)
        sel_amazon = "select cdate,app_key,version,nation," \
                     "avg(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end) as app_create_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end) as kb_create_1st_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end) as kb_createview_startup_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end) as kb_createview_startup_1st_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end) as kb_warm_startup_time," \
                     "avg(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end) as emoji_time," \
                     "avg(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end) as menu_time," \
                     "avg(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end) as suggestions_time, " \
                     "avg(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end) as slide_suggestions_time, " \
                     "" \
                     "approx_percentile(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end,0.95) as app_create_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end,0.95) as kb_create_1st_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end,0.95) as kb_createview_startup_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end,0.95) as kb_createview_startup_1st_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end,0.95) as kb_warm_startup_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end,0.95) as emoji_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end,0.95) as menu_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end,0.95) as suggestions_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end,0.95) as slide_suggestions_time_95 " \
                     "from gimbal.event where app_key='" + appkey + "' and layout='performance' and item='key_metrics' and cdate=" + cdate_2 + "  and event_part='other'  and version in " + verson_str + " and nation in ('us','br','id','mx','ph','ru','ar','co','fr','es') group by cdate,app_key,version,nation"

        insert_sql_value_list = []

        count = 4
        while count > 0:
            count -= 1
            print(appkey, athena_shell + "\"" + sel_amazon + "\"")
            p = subprocess.Popen(athena_shell + "\"" + sel_amazon + "\"", shell=True, stdout=subprocess.PIPE)
            subprocess.Popen.wait(p)
            result = p.stdout.readlines()
            print("result: " + str(result))
            if len(result) > 0:
                for line in result:
                    line = line.decode()
                    if cdate_2 in line:
                        line = line.replace("\n", "")
                        valuses = line.split("\t")
                        temp_sql_value = "("
                        if len(valuses) == count_key:
                            for i in range(count_key):
                                if valuses[i] == "null":
                                    temp_sql_value += "null,"
                                else:
                                    temp_sql_value += "'" + str(valuses[i]) + "',"
                            temp_sql_value = temp_sql_value[:-1] + ")"
                            insert_sql_value_list.append(temp_sql_value)
                break
        insert_sql = "INSERT INTO `keyboard_tech` (`create_date`, `app_key`, `version`, `nation`, `app_create_time`, " \
                     "`kb_create_1st_time`, `kb_createview_startup_time`, `kb_createview_startup_1st_time`, `kb_warm_startup_time`, " \
                     "`emoji_time`, `menu_time`, `suggestions_time`, `slide_suggestions_time`, `app_create_time_95`, `kb_create_1st_time_95`," \
                     "`kb_createview_startup_time_95`,`kb_createview_startup_1st_time_95`, `kb_warm_startup_time_95`, `emoji_time_95`, " \
                     "`menu_time_95`, `suggestions_time_95`,`slide_suggestions_time_95`) VALUES "

        for sql_val in insert_sql_value_list:
            insert_sql += sql_val + ","
            #
            # Execute SQL
        insert_sql = insert_sql[:-1]
        print(insert_sql)
        try:
            cursor.execute(insert_sql)
            db.commit()
        except:
            db.rollback()


def test_insert_all_ver():
    cdate_2 = get_cdate(2)

    """
    1. 定义top10国家
       'us','br','id','mx','ph','ru','ar','co','fr','es'

    2. 定义top5版本
       172.31.21.163
       koala
       app_details_dlu

       kika top5版本
       select g_key,g_value from app_details_dlu where create_date='2017-06-21' and app_key='78472ddd7528bcacc15725a16aeec190' and gby_type='appversion' and g_value>1000 order by g_key desc limit 5;
       pro top5
       select g_key,g_value from app_details_dlu where create_date='2017-06-21' and app_key='4e5ab3a6d2140457e0423a28a094b1fd' and gby_type='appversion' and g_value>1000 order by g_key desc limit 5;
       ikey top5
       select g_key,g_value from app_details_dlu where create_date='2017-06-21' and app_key='e2934742f9d3b8ef2b59806a041ab389' and gby_type='appversion' and g_value>1000 order by g_key desc limit 5;

    3.分APPKEY查询

      select cdate,app_key,version,nation,
      approx_percentile(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' then cast( json_extract_scalar(extra,'$.app_create') as INTEGER) end) as app_create_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_create_1st') as INTEGER) end) as kb_create_1st_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup') as INTEGER) end) as kb_createview_startup_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as INTEGER) end) as kb_createview_startup_1st_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_warm_startup') as INTEGER) end) as kb_warm_startup_time,
      approx_percentile(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' then cast( json_extract_scalar(extra,'$.emoji_time') as INTEGER) end) as emoji_time,
      approx_percentile(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' then cast( json_extract_scalar(extra,'$.menu_time') as INTEGER) end) as emoji_time,
      approx_percentile(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.suggestions_time') as INTEGER) end) as suggestions_time,
      approx_percentile(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as INTEGER) end) as slide_suggestions_time from event where app_key='XXXXXXX' and cdate=XXXXXXXXX  and event_part='other'  and version in ('XX','XX','XX') and nation in ('us','br','id','mx','ph','ru','ar','co','fr','es') group by cdate,app_key,version,nation



    Keyboard_Tech

    有两种方式将指标入库
    1.    程序
           MySQL地址为：
           AWS内网IP：172.31.21.163
           db_nme: kika_key_metric
           TableName: keyboard_tech
           user:  kikatechuser
           pwd:  r9ca#J40cd39

           可读可写可删可以更改表结构

    2.    Web访问   http://phpadmin.kika-backend.com/
           用户名密码同上，登陆后选择数据库和表，在Insert处可以插入数据。(以下表名只是示范，每个账号只能看到自己的表）
           Inline image 1

    """
    k1 = '\'78472ddd7528bcacc15725a16aeec190\''
    k2 = '\'4e5ab3a6d2140457e0423a28a094b1fd\''
    k3 = '\'e2934742f9d3b8ef2b59806a041ab389\''
    k4 = '\'b1f6dd09ec315aa442bbb01d0663dd22\''
    k5 = "\'73750b399064a5eb43afc338cd5cad25\'"

    db_kola = pymysql.connect(host="172.31.21.163", user="kikatechuser", passwd="r9ca#J40cd39", db="koala")

    db = pymysql.connect(host="172.31.21.163", user="kikatechuser", passwd="r9ca#J40cd39", db="kika_key_metrics")

    cursor = db.cursor()

    """
    create_date	app_key	version	nation	app_create_time	kb_create_1st_time	kb_createview_startup_time	kb_createview_startup_1st_time	kb_warm_startup_time	emoji_time	menu_time	suggestions_time	slide_suggestions_time
    """
    insert_keys = ["create_date", "app_key", "app_create_time", "kb_create_1st_time",
                   "kb_createview_startup_time",
                   "kb_createview_startup_1st_time", "kb_warm_startup_time", "emoji_time", "menu_time",
                   "suggestions_time", "slide_suggestions_time",
                   "app_create_time_95", "kb_create_1st_time_95",
                   "kb_createview_startup_time_95", "kb_createview_startup_1st_time_95", "kb_warm_startup_time_95",
                   "emoji_time_95", "menu_time_95", "suggestions_time_95",
                   "slide_suggestions_time_95"]
    count_key = len(insert_keys)

    for appkey in [k1, k2, k3, k4, k5]:
        sel_amazon = "select cdate,app_key," \
                     "avg(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end) as app_create_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end) as kb_create_1st_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end) as kb_createview_startup_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end) as kb_createview_startup_1st_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end) as kb_warm_startup_time," \
                     "avg(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end) as emoji_time," \
                     "avg(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end) as menu_time," \
                     "avg(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end) as suggestions_time, " \
                     "avg(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end) as slide_suggestions_time, " \
                     "" \
                     "approx_percentile(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end,0.95) as app_create_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end,0.95) as kb_create_1st_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end,0.95) as kb_createview_startup_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end,0.95) as kb_createview_startup_1st_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end,0.95) as kb_warm_startup_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end,0.95) as emoji_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end,0.95) as menu_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end,0.95) as suggestions_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end,0.95) as slide_suggestions_time_95 " \
                     "from gimbal.event where app_key=" + appkey + " and layout='performance' and item='key_metrics' and cdate=" + cdate_2 + "  and event_part='other' group by cdate,app_key"

        insert_sql_value_list = []

        count = 4
        while count > 0:
            count -= 1
            print(appkey, athena_shell + "\"" + sel_amazon + "\"")
            p = subprocess.Popen(athena_shell + "\"" + sel_amazon + "\"", shell=True, stdout=subprocess.PIPE)
            subprocess.Popen.wait(p)
            result = p.stdout.readlines()
            print("result: " + str(result))
            if len(result) > 0:
                for line in result:
                    line = line.decode()
                    if cdate_2 in line:
                        line = line.replace("\n", "")
                        valuses = line.split("\t")
                        temp_sql_value = "('all', 'all', "
                        if len(valuses) == count_key:
                            for i in range(count_key):
                                if valuses[i] == "null":
                                    temp_sql_value += "null,"
                                else:
                                    temp_sql_value += "'" + str(valuses[i]) + "',"
                            temp_sql_value = temp_sql_value[:-1] + ")"
                            insert_sql_value_list.append(temp_sql_value)
                break
        insert_sql = "INSERT INTO `keyboard_tech` (`version`,`nation`, `create_date`, `app_key`, `app_create_time`, " \
                     "`kb_create_1st_time`, `kb_createview_startup_time`, `kb_createview_startup_1st_time`, `kb_warm_startup_time`, " \
                     "`emoji_time`, `menu_time`, `suggestions_time`, `slide_suggestions_time`, `app_create_time_95`, `kb_create_1st_time_95`," \
                     "`kb_createview_startup_time_95`,`kb_createview_startup_1st_time_95`, `kb_warm_startup_time_95`, `emoji_time_95`, " \
                     "`menu_time_95`, `suggestions_time_95`,`slide_suggestions_time_95`) VALUES "

        for sql_val in insert_sql_value_list:
            insert_sql += sql_val + ","
            #
            # Execute SQL
        insert_sql = insert_sql[:-1]
        print(insert_sql)
        try:
            cursor.execute(insert_sql)
            db.commit()
        except:
            db.rollback()


def test_insert_latest_and_nations():
    cdate_2 = get_cdate(2)
    cdate_2_format = cdate_2[0:4] + '-' + cdate_2[4:6] + '-' + cdate_2[6:]

    """
    1. 定义top10国家
       'us','br','id','mx','ph','ru','ar','co','fr','es'

    2. 定义top5版本
       172.31.21.163
       koala
       app_details_dlu

       kika top5版本
       select g_key,g_value from app_details_dlu where create_date='2017-06-21' and app_key='78472ddd7528bcacc15725a16aeec190' and gby_type='appversion' and g_value>1000 order by g_key desc limit 5;
       pro top5
       select g_key,g_value from app_details_dlu where create_date='2017-06-21' and app_key='4e5ab3a6d2140457e0423a28a094b1fd' and gby_type='appversion' and g_value>1000 order by g_key desc limit 5;
       ikey top5
       select g_key,g_value from app_details_dlu where create_date='2017-06-21' and app_key='e2934742f9d3b8ef2b59806a041ab389' and gby_type='appversion' and g_value>1000 order by g_key desc limit 5;

    3.分APPKEY查询

      select cdate,app_key,version,nation,
      approx_percentile(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' then cast( json_extract_scalar(extra,'$.app_create') as INTEGER) end) as app_create_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_create_1st') as INTEGER) end) as kb_create_1st_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup') as INTEGER) end) as kb_createview_startup_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as INTEGER) end) as kb_createview_startup_1st_time,
      approx_percentile(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_warm_startup') as INTEGER) end) as kb_warm_startup_time,
      approx_percentile(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' then cast( json_extract_scalar(extra,'$.emoji_time') as INTEGER) end) as emoji_time,
      approx_percentile(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' then cast( json_extract_scalar(extra,'$.menu_time') as INTEGER) end) as emoji_time,
      approx_percentile(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.suggestions_time') as INTEGER) end) as suggestions_time,
      approx_percentile(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as INTEGER) end) as slide_suggestions_time from event where app_key='XXXXXXX' and cdate=XXXXXXXXX  and event_part='other'  and version in ('XX','XX','XX') and nation in ('us','br','id','mx','ph','ru','ar','co','fr','es') group by cdate,app_key,version,nation



    Keyboard_Tech

    有两种方式将指标入库
    1.    程序
           MySQL地址为：
           AWS内网IP：172.31.21.163
           db_nme: kika_key_metric
           TableName: keyboard_tech
           user:  kikatechuser
           pwd:  r9ca#J40cd39

           可读可写可删可以更改表结构

    2.    Web访问   http://phpadmin.kika-backend.com/
           用户名密码同上，登陆后选择数据库和表，在Insert处可以插入数据。(以下表名只是示范，每个账号只能看到自己的表）
           Inline image 1

    """

    db_kola = pymysql.connect(host="172.31.21.163", user="kikatechuser", passwd="r9ca#J40cd39", db="koala")

    db = pymysql.connect(host="172.31.21.163", user="kikatechuser", passwd="r9ca#J40cd39", db="kika_key_metrics")

    select_version = {
        # kika top5版本
        "78472ddd7528bcacc15725a16aeec190": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='78472ddd7528bcacc15725a16aeec190' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 1;",
        # pro top5
        "4e5ab3a6d2140457e0423a28a094b1fd": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='4e5ab3a6d2140457e0423a28a094b1fd' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 1;",
        # ikey top5
        "e2934742f9d3b8ef2b59806a041ab389": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='e2934742f9d3b8ef2b59806a041ab389' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 1;",
        # kikaindic top5
        "b1f6dd09ec315aa442bbb01d0663dd22": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='b1f6dd09ec315aa442bbb01d0663dd22' and gby_type='appversion' and g_value>1000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 1;"
    }

    cursor = db.cursor()
    cursor_kola = db_kola.cursor()

    """
    create_date	app_key	version	nation	app_create_time	kb_create_1st_time	kb_createview_startup_time	kb_createview_startup_1st_time	kb_warm_startup_time	emoji_time	menu_time	suggestions_time	slide_suggestions_time
    """
    insert_keys = ["create_date", "app_key", "nation", "app_create_time", "kb_create_1st_time",
                   "kb_createview_startup_time",
                   "kb_createview_startup_1st_time", "kb_warm_startup_time", "emoji_time", "menu_time",
                   "suggestions_time", "slide_suggestions_time",
                   "app_create_time_95", "kb_create_1st_time_95",
                   "kb_createview_startup_time_95", "kb_createview_startup_1st_time_95", "kb_warm_startup_time_95",
                   "emoji_time_95", "menu_time_95", "suggestions_time_95",
                   "slide_suggestions_time_95"]
    count_key = len(insert_keys)
    for appkey, sel_ver in select_version.items():
        try:
            cursor_kola.execute(sel_ver)
            db_kola.commit()
            result = cursor_kola.fetchall()
        except:
            continue
        verson_str = "("
        for g_key, g_value in result:
            verson_str += "'" + str(g_key) + "',"
        verson_str = verson_str[:-1] + ")"
        print(verson_str)
        sel_amazon = "select cdate,app_key,nation," \
                     "avg(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end) as app_create_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end) as kb_create_1st_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end) as kb_createview_startup_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end) as kb_createview_startup_1st_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end) as kb_warm_startup_time," \
                     "avg(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end) as emoji_time," \
                     "avg(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end) as menu_time," \
                     "avg(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end) as suggestions_time, " \
                     "avg(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end) as slide_suggestions_time, " \
                     "" \
                     "approx_percentile(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end,0.95) as app_create_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end,0.95) as kb_create_1st_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end,0.95) as kb_createview_startup_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end,0.95) as kb_createview_startup_1st_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end,0.95) as kb_warm_startup_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end,0.95) as emoji_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end,0.95) as menu_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end,0.95) as suggestions_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end,0.95) as slide_suggestions_time_95 " \
                     "from gimbal.event where app_key='" + appkey + "' and layout='performance' and item='key_metrics' and cdate=" + cdate_2 + "  and event_part='other'  and version in " + verson_str + " and nation in ('us','br','id','mx','ph','ru','ar','co','fr','es') group by cdate,app_key,version,nation"

        insert_sql_value_list = []

        count = 4
        while count > 0:
            count -= 1
            print(appkey, athena_shell + "\"" + sel_amazon + "\"")
            p = subprocess.Popen(athena_shell + "\"" + sel_amazon + "\"", shell=True, stdout=subprocess.PIPE)
            subprocess.Popen.wait(p)
            result = p.stdout.readlines()
            print("result: " + str(result))
            if len(result) > 0:
                for line in result:
                    line = line.decode()
                    if cdate_2 in line:
                        line = line.replace("\n", "")
                        valuses = line.split("\t")
                        temp_sql_value = "('latest', "
                        if len(valuses) == count_key:
                            for i in range(count_key):
                                if valuses[i] == "null":
                                    temp_sql_value += "null,"
                                else:
                                    temp_sql_value += "'" + str(valuses[i]) + "',"
                            temp_sql_value = temp_sql_value[:-1] + ")"
                            insert_sql_value_list.append(temp_sql_value)
                break
        insert_sql = "INSERT INTO `keyboard_tech` (`version`, `create_date`, `app_key`, `nation`, `app_create_time`, " \
                     "`kb_create_1st_time`, `kb_createview_startup_time`, `kb_createview_startup_1st_time`, `kb_warm_startup_time`, " \
                     "`emoji_time`, `menu_time`, `suggestions_time`, `slide_suggestions_time`, `app_create_time_95`, `kb_create_1st_time_95`," \
                     "`kb_createview_startup_time_95`,`kb_createview_startup_1st_time_95`, `kb_warm_startup_time_95`, `emoji_time_95`, " \
                     "`menu_time_95`, `suggestions_time_95`,`slide_suggestions_time_95`) VALUES "

        for sql_val in insert_sql_value_list:
            insert_sql += sql_val + ","
            #
            # Execute SQL
        insert_sql = insert_sql[:-1]
        print(insert_sql)
        try:
            cursor.execute(insert_sql)
            db.commit()
        except:
            db.rollback()



def test_insert_latest():
    cdate_2 = get_cdate(2)
    cdate_2_format = cdate_2[0:4] + '-' + cdate_2[4:6] + '-' + cdate_2[6:]

    db_kola = pymysql.connect(host="172.31.21.163", user="kikatechuser", passwd="r9ca#J40cd39", db="koala")

    db = pymysql.connect(host="172.31.21.163", user="kikatechuser", passwd="r9ca#J40cd39", db="kika_key_metrics")

    select_version = {
        # kika top5版本
        "78472ddd7528bcacc15725a16aeec190": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='78472ddd7528bcacc15725a16aeec190' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED)  desc limit 1;",
        # pro top5
        "4e5ab3a6d2140457e0423a28a094b1fd": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='4e5ab3a6d2140457e0423a28a094b1fd' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED)  desc limit 1;",
        # ikey top5
        "e2934742f9d3b8ef2b59806a041ab389": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='e2934742f9d3b8ef2b59806a041ab389' and gby_type='appversion' and g_value>10000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED)  desc limit 1;",
        # clavier top5
        "73750b399064a5eb43afc338cd5cad25": "select g_key,g_value from app_details_dlu where create_date='" + cdate_2_format + "' and app_key='73750b399064a5eb43afc338cd5cad25' and gby_type='appversion' and g_value>1000 order by CAST(SUBSTRING_INDEX(g_key, '.', -1) AS UNSIGNED) desc limit 1;"

    }

    cursor = db.cursor()
    cursor_kola = db_kola.cursor()

    """
    create_date	app_key	version	nation	app_create_time	kb_create_1st_time	kb_createview_startup_time	kb_createview_startup_1st_time	kb_warm_startup_time	emoji_time	menu_time	suggestions_time	slide_suggestions_time
    """
    insert_keys = ["create_date", "app_key", "app_create_time", "kb_create_1st_time", "kb_createview_startup_time",
                   "kb_createview_startup_1st_time", "kb_warm_startup_time", "emoji_time", "menu_time",
                   "suggestions_time", "slide_suggestions_time",
                   "app_create_time_95", "kb_create_1st_time_95",
                   "kb_createview_startup_time_95", "kb_createview_startup_1st_time_95", "kb_warm_startup_time_95",
                   "emoji_time_95", "menu_time_95", "suggestions_time_95",
                   "slide_suggestions_time_95"]
    count_key = len(insert_keys)
    for appkey, sel_ver in select_version.items():
        try:
            cursor_kola.execute(sel_ver)
            db_kola.commit()
            result = cursor_kola.fetchall()
        except:
            continue
        verson_str = "("
        for g_key, g_value in result:
            verson_str += "'" + str(g_key) + "',"
        verson_str = verson_str[:-1] + ")"
        print(verson_str)
        sel_amazon = "select cdate,app_key," \
                     "avg(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.app_create') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end) as app_create_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end) as kb_create_1st_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end) as kb_createview_startup_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end) as kb_createview_startup_1st_time," \
                     "avg(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end) as kb_warm_startup_time," \
                     "avg(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end) as emoji_time," \
                     "avg(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.menu_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end) as menu_time," \
                     "avg(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)>0 and cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT)<20000 then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end) as suggestions_time, " \
                     "avg(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) >0 and cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) <20000 then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end) as slide_suggestions_time, " \
                     "" \
                     "approx_percentile(case when json_extract_scalar(extra,'$.app_create') is not null and  json_extract_scalar(extra,'$.app_create')<>'' then cast( json_extract_scalar(extra,'$.app_create') as BIGINT) end,0.95) as app_create_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_create_1st') is not null and  json_extract_scalar(extra,'$.kb_create_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_create_1st') as BIGINT) end,0.95) as kb_create_1st_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup') is not null and  json_extract_scalar(extra,'$.kb_createview_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup') as BIGINT) end,0.95) as kb_createview_startup_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_createview_startup_1st') is not null and  json_extract_scalar(extra,'$.kb_createview_startup_1st')<>'' then cast( json_extract_scalar(extra,'$.kb_createview_startup_1st') as BIGINT) end,0.95) as kb_createview_startup_1st_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.kb_warm_startup') is not null and  json_extract_scalar(extra,'$.kb_warm_startup')<>'' then cast( json_extract_scalar(extra,'$.kb_warm_startup') as BIGINT) end,0.95) as kb_warm_startup_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.emoji_time') is not null and  json_extract_scalar(extra,'$.emoji_time')<>'' then cast( json_extract_scalar(extra,'$.emoji_time') as BIGINT) end,0.95) as emoji_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.menu_time') is not null and  json_extract_scalar(extra,'$.menu_time')<>'' then cast( json_extract_scalar(extra,'$.menu_time') as BIGINT) end,0.95) as menu_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.suggestions_time') is not null and  json_extract_scalar(extra,'$.suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.suggestions_time') as BIGINT) end,0.95) as suggestions_time_95," \
                     "approx_percentile(case when json_extract_scalar(extra,'$.slide_suggestions_time') is not null and  json_extract_scalar(extra,'$.slide_suggestions_time')<>'' then cast( json_extract_scalar(extra,'$.slide_suggestions_time') as BIGINT) end,0.95) as slide_suggestions_time_95 " \
                     "from gimbal.event where app_key='" + appkey + "' and layout='performance' and item='key_metrics' and cdate=" + cdate_2 + "  and event_part='other'  and version in " + verson_str + " group by cdate,app_key,version"

        insert_sql_value_list = []

        count = 4
        while count > 0:
            count -= 1
            print(appkey, athena_shell + "\"" + sel_amazon + "\"")
            p = subprocess.Popen(athena_shell + "\"" + sel_amazon + "\"", shell=True, stdout=subprocess.PIPE)
            subprocess.Popen.wait(p)
            result = p.stdout.readlines()
            print("result: " + str(result))
            if len(result) > 0:
                for line in result:
                    line = line.decode()
                    if cdate_2 in line:
                        line = line.replace("\n", "")
                        valuses = line.split("\t")
                        temp_sql_value = verson_str[:-1] + ", 'all', "
                        if len(valuses) == count_key:
                            for i in range(count_key):
                                if valuses[i] == "null":
                                    temp_sql_value += "null,"
                                else:
                                    temp_sql_value += "'" + str(valuses[i]) + "',"
                            temp_sql_value = temp_sql_value[:-1] + ")"
                            insert_sql_value_list.append(temp_sql_value)
                break
        insert_sql = "INSERT INTO `keyboard_tech` (`version`, `nation`, `create_date`, `app_key`, `app_create_time`, " \
                     "`kb_create_1st_time`, `kb_createview_startup_time`, `kb_createview_startup_1st_time`, `kb_warm_startup_time`, " \
                     "`emoji_time`, `menu_time`, `suggestions_time`, `slide_suggestions_time`, `app_create_time_95`, `kb_create_1st_time_95`," \
                     "`kb_createview_startup_time_95`,`kb_createview_startup_1st_time_95`, `kb_warm_startup_time_95`, `emoji_time_95`, " \
                     "`menu_time_95`, `suggestions_time_95`,`slide_suggestions_time_95`) VALUES "

        for sql_val in insert_sql_value_list:
            insert_sql += sql_val + ","
            #
            # Execute SQL
        insert_sql = insert_sql[:-1]
        print(insert_sql)
        try:
            cursor.execute(insert_sql)
            db.commit()
        except:
            db.rollback()


if __name__ == '__main__':
    test_insert_top5_and_nations()
    test_insert_all_ver()
    # test_insert_latest_and_nations()
    test_insert_latest()
