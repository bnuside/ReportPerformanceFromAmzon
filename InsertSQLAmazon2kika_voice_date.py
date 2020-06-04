#!/usr/bin/python
# -*- coding: UTF-8 -*-

import subprocess
import pymysql
import time
import datetime


athena_shell = "/usr/local/jdk1.8.0_51/bin/java -classpath /home/pubsrv/project/athenaaccess/libs/athena_query_kika_tool-1.0.jar:" \
          "/home/pubsrv/project/athenaaccess/libs/AthenaJDBC41-1.0.0.jar:" \
          "/home/pubsrv/project/athenaaccess/libs/mysql-connector-java-5.1.40-bin.jar" \
          " com.kika.tech.athena_query_kika_tool.AthenaQueryTool" \
          " AKIAQFJUI6DZEXYXUKXO "

k1='\'78472ddd7528bcacc15725a16aeec190\''
k2='\'4e5ab3a6d2140457e0423a28a094b1fd\''
k3='\'e2934742f9d3b8ef2b59806a041ab389\''


def get_cdate(i):
    time_today = time.localtime(time.time())
    year = time_today.tm_year
    mon = time_today.tm_mon
    day = time_today.tm_mday
    # year = 2017
    # mon = 7
    # day = 25
    return str(datetime.datetime(year, mon, day) - datetime.timedelta(i)).split()[0].replace('-', '')


def test_voice_default_date():
    time_now = time.localtime(time.time())
    cdate_2 = get_cdate(2)
    cdate_3 = get_cdate(3)
    # cdate_2 = "20170622"
    # cdate_3 = "20170621"

    sql_list = [
    # --数据查询
    # usr/local/jdk1.8.0_51/bin/java -classpath /home/pubsrv/project/athenaaccess/libs/athena_query_kika_tool-1.0.jar:/home/pubsrv/project/athenaaccess/libs/AthenaJDBC41-1.0.0.jar:/home/pubsrv/project/athenaaccess/libs/mysql-connector-java-5.1.40-bin.jar com.kika.tech.athena_query_kika_tool.AthenaQueryTool AKIAQFJUI6DZEXYXUKXO
    #
    # 1.
    # --用户数据统计
        (["create_date", "app_key", "us_uv", "us_pv", "voicekey_uv", "voicekey_pv", "voiceui_uv", "voiceui_pv", "voicekeyui_uv", "voicekeyui_pv"], "select cdate, app_key, count(distinct case when lower(lang)='en_us' then deviceuid end) as us_uv, count(case when lower(lang)='en_us' then deviceuid end) as us_pv, count(distinct case when json_extract_scalar(extra,'$.isSeparatorVoiceKey')='true' then deviceuid end) as voicekey_uv, count(case when json_extract_scalar(extra,'$.isSeparatorVoiceKey')='true' then deviceuid end) as voicekey_pv, count(distinct case when json_extract_scalar(extra,'$.isNewVoice')='true' then deviceuid end) as voiceui_uv, count(case when json_extract_scalar(extra,'$.isNewVoice')='true' then deviceuid end) as voiceui_pv, count(distinct case when json_extract_scalar(extra,'$.isSeparatorVoiceKey')='true' and json_extract_scalar(extra,'$.isNewVoice')='true' then deviceuid end) as voicekeyui_uv, count(case when json_extract_scalar(extra,'$.isSeparatorVoiceKey')='true' and json_extract_scalar(extra,'$.isNewVoice')='true' then deviceuid end) as voicekeyui_pv from gimbal.event where app_key='78472ddd7528bcacc15725a16aeec190' and layout='layout_kika_voice_new' and event_part='other' and item='voicekey' and cdate="+cdate_2+" group by app_key, cdate;"),

    # 2.
    # --进入新UI的统计
    # --点击单独Voice Key统计
    # --dismiss统计
    # --trace统计
        (["popshow_uv", "popshow_pv","clickvoicekey_uv", "clickvoicekey_pv", "dismiss_uv", "dismiss_pv", "trace_uv"], "select count(distinct case when layout='layout_kika_voice_new'and event_part='other' and item='pop_show' then deviceuid end) as popshow_uv, count(case when layout='layout_kika_voice_new'and event_part='other' and item='pop_show' then deviceuid end) as popshow_pv, count(distinct case when layout='keyboard' and event_part='keyboard' and item='voice_key' then deviceuid end) as clickvoicekey_uv, count(case when layout='keyboard' and event_part='keyboard' and item='voice_key' then deviceuid end) as clickvoicekey_pv, count(distinct case when layout='layout_kika_voice_new'and event_part='other' and item='pop_dismiss' then deviceuid end) as dismiss_uv, count(case when layout='layout_kika_voice_new'and event_part='other' and item='pop_dismiss' then deviceuid end) as dismiss_pv, count(distinct case when layout='layout_kika_voice_new'and event_part='other' and item='trace' then deviceuid end) as trace_uv from gimbal.event where app_key='78472ddd7528bcacc15725a16aeec190' and cdate="+cdate_2+" group by app_key, cdate;"),

    # 3.
    # --点击单独Voice key，进入新版Voice UI
        (["uv"],"select count(distinct a.deviceuid) as uv from (select distinct deviceuid from gimbal.event where app_key='78472ddd7528bcacc15725a16aeec190' and layout='layout_kika_voice_new' and event_part='other' and item='pop_show' and cdate=" + cdate_2 + " )a join (select distinct deviceuid from gimbal.event where app_key='78472ddd7528bcacc15725a16aeec190' and layout='keyboard' and event_part='keyboard' and item='voice_key' and cdate=" + cdate_2 + " )b on a.deviceuid=b.deviceuid ;"),

    #
    #
    # 4.
    # --有效语音输入，进入大于2秒（非误点）
    # --语音输入使用的总时长
    # --语音输入使用时长平均时长
    # --语音输入使用时长中位数
    # --用户点击暂停统计
    # --用户点击删除统计
    # --用户删除总词数
    # --用户删除平均词数
    # --用户删除中位数
        (["validvoice_uv", "validvoice_pv", "duration_total", "duration_avg", "duration_median", "clickpause_uv", "clickpause_pv", "clickdel_uv", "clickdel_pv", "delword_total", "delword_avg", "delword_median"], "select count(distinct deviceuid) as validvoice_uv, count(deviceuid) as validvoice_pv, sum(cast(json_extract_scalar(extra,'$.duration') as bigint)) as duration_total, avg(cast(json_extract_scalar(extra,'$.duration') as bigint)) as duration_avg, approx_percentile(cast(json_extract_scalar(extra,'$.duration') as bigint), 0.5) as duration_median, count(distinct case when cast(json_extract_scalar(extra,'$.pause_count') as bigint)>=1 then deviceuid end) as clickpause_uv, count(case when cast(json_extract_scalar(extra,'$.pause_count') as bigint)>=1 then deviceuid end) as clickpause_pv, count(distinct case when cast(json_extract_scalar(extra,'$.total_delete_word_num') as bigint)>=1 then deviceuid end) as clickdel_uv, count(case when cast(json_extract_scalar(extra,'$.total_delete_word_num') as bigint)>=1 then deviceuid end) as clickdel_pv, sum(cast(json_extract_scalar(extra,'$.total_delete_word_num') as bigint)) as delword_total, avg(cast(json_extract_scalar(extra,'$.total_delete_word_num') as bigint)) as delword_avg, approx_percentile(cast(json_extract_scalar(extra,'$.total_delete_word_num') as bigint), 0.5) as delword_median from gimbal.event where app_key='78472ddd7528bcacc15725a16aeec190' and layout='layout_kika_voice_new' and event_part='other' and item='pop_dismiss' and cdate=" + cdate_2 +" and cast(json_extract_scalar(extra,'$.duration') as bigint)>2000 group by app_key, cdate;"),
        ]


    """
    KeyboardProduct

    有两种方式将指标入库
    1.    程序
           MySQL地址为：
           172.31.21.163
            db: kika_tableau
            table: voice_daily
            user: voice_daily_user
            pwd: E7lIxUjJr97P

           可读可写可删可以更改表结构
    """
    db = pymysql.connect(host="172.31.21.163", user="voice_daily_user", passwd="E7lIxUjJr97P", db="kika_tableau")

    cursor =db.cursor()
    insert_sql_dict_list = {}
    for item in sql_list:
        result = []
        count = 3
        while count > 0:
            count -= 1
            keys, sql = item
            print(keys, athena_shell + "\"" + sql + "\"")
            p = subprocess.Popen(athena_shell + "\"" + sql + "\"", shell=True, stdout=subprocess.PIPE)
            subprocess.Popen.wait(p)
            result = p.stdout.readlines()
            count_key = len(keys)
            print("result: " + str(result))
            if len(result) > 0:
                for line in result:
                    line = line.decode()
                    line = line.replace("\n", "")
                    valuses = line.split("\t")
                    if count_key == len(valuses):
                        for i in range(count_key):
                            insert_sql_dict_list[keys[i]] = valuses[i]
                break
            time.sleep(5)

    insert_sql = "INSERT INTO `voice_daily` (`"
    for key_name in insert_sql_dict_list.keys():
        insert_sql = insert_sql + key_name + "`,`"
    insert_sql = insert_sql[:-2] + ") VALUES "
    val_sql = "('"
    for val in insert_sql_dict_list.values():
        val_sql = val_sql + val + "','"
    val_sql = val_sql[:-2] + ")"
    insert_sql = insert_sql + val_sql
    print("insert: sql: " + insert_sql)
    my_sql = insert_sql


    #
    # Execute SQL
    try:
        cursor.execute(my_sql)
        db.commit()
    except:
        db.rollback()


def test_voice_update_data_before():
    """
    :return:
    """

    time_now = time.localtime(time.time())
    cdate_start = get_cdate(9)
    cdate_re1 = get_cdate(8)
    cdate_re3 = get_cdate(6)
    cdate_re7 = get_cdate(2)

    sql_list = [
    # voice retention: new user, 1-day
    (["voice_new", "voice_new_ret1d"], "select count(distinct a.deviceuid) as voice_new, count(distinct d.deviceuid) as voice_new_ret1d from ((select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.dnu where app_key = '78472ddd7528bcacc15725a16aeec190' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_start + ")a  join(select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_start + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)b on a.deviceuid = b.deviceuid) left join (select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_re1 + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)d on a.deviceuid = d.deviceuid;"),
    # voice retention: new user, 3-day
    (["voice_new", "voice_new_ret3d"], "select count(distinct a.deviceuid) as voice_new, count(distinct d.deviceuid) as voice_new_ret3d from ((select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.dnu where app_key = '78472ddd7528bcacc15725a16aeec190' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_start + ")a join( select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_start + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)b on a.deviceuid = b.deviceuid) left join( select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_re3 + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)d on a.deviceuid = d.deviceuid;"),
    # voice retention: new user, 7-day
    (["voice_new", "voice_new_ret7d"], "select count(distinct a.deviceuid) as voice_new, count(distinct d.deviceuid) as voice_new_ret7d from ((select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.dnu where app_key = '78472ddd7528bcacc15725a16aeec190' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_start + ")a join( select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_start + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)b on a.deviceuid = b.deviceuid) left join( select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_re7 + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)d on a.deviceuid = d.deviceuid;"),
    # voice retention: old user, 1-day
    (["voice_old", "voice_old_ret1d"], "select count(distinct b.deviceuid) as voice_old, count(distinct d.deviceuid) as voice_old_ret1d from (select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_start + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)b left join( select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_re1 + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)d on b.deviceuid = d.deviceuid;"),
    # voice retention: old user, 3-day
    (["voice_old", "voice_old_ret3d"], "select count(distinct b.deviceuid) as voice_old, count(distinct d.deviceuid) as voice_old_ret3d from (select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_start + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)b left join( select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_re3 + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)d on b.deviceuid = d.deviceuid;"),
    # voice retention: old user, 7-day
    (["voice_old", "voice_old_ret7d"], "select count(distinct b.deviceuid) as voice_old, count(distinct d.deviceuid) as voice_old_ret3d from (select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_start + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)b left join( select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and event_part = 'other' and item = 'pop_dismiss' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_re7 + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000)d on b.deviceuid = d.deviceuid;"),
    ]

    """
        KeyboardProduct

        有两种方式将指标入库
        1.    程序
               MySQL地址为：
               172.31.21.163
                db: kika_tableau
                table: voice_daily
                user: voice_daily_user
                pwd: E7lIxUjJr97P

               可读可写可删可以更改表结构
        """
    db = pymysql.connect(host="172.31.21.163", user="voice_daily_user", passwd="E7lIxUjJr97P", db="kika_tableau")

    cursor = db.cursor()
    insert_sql_dict_list = {}
    for item in sql_list:
        result = []
        count = 3
        while count > 0:
            count -= 1
            keys, sql = item
            print(keys, athena_shell + "\"" + sql + "\"")
            p = subprocess.Popen(athena_shell + "\"" + sql + "\"", shell=True, stdout=subprocess.PIPE)
            subprocess.Popen.wait(p)
            result = p.stdout.readlines()
            count_key = len(keys)
            print("result: " + str(result))
            if len(result) > 0:
                for line in result:
                    line = line.decode()
                    line = line.replace("\n", "")
                    valuses = line.split("\t")
                    if count_key == len(valuses):
                        for i in range(count_key):
                            insert_sql_dict_list[keys[i]] = valuses[i]
                break
            time.sleep(5)

    update_sql = "UPDATE `voice_daily` SET "
    for key, val in insert_sql_dict_list.items():
        update_sql = update_sql + "`" + str(key) + "` = '" + str(val) + "',"
    update_sql = update_sql[:-1] + " WHERE "
    update_sql += "`create_date` = '" + cdate_start + "' and `app_key` = " + k1
    my_sql = update_sql
    print("Update: sql: " + update_sql)

    #
    # Execute SQL
    try:
        cursor.execute(my_sql)
        db.commit()
    except:
        db.rollback()

def test_voice_update_date():
    return 0


def test_voice_daily_v2():
    """
    :return:
    """
    # cdate_3 = get_cdate(3)
    # cdate_2 = get_cdate(2)

    """
        KeyboardProduct

        有两种方式将指标入库
        1.    程序
               MySQL地址为：
               172.31.21.163
                db: kika_tableau
                table: voice_daily
                user: voice_daily_user
                pwd: E7lIxUjJr97P

               可读可写可删可以更改表结构
        """
    db = pymysql.connect(host="172.31.21.163", user="voice_daily_user", passwd="E7lIxUjJr97P", db="kika_tableau")

    sql_list = [
    # 新版本Kika
    # UI的数量（分母）
    #  # 当前日期减3
    (["all_uv", "all_pv", "voiceui_uv", "voiceui_pv"], "select count(distinct deviceuid) as all_uv, count(deviceuid) as all_pv, count(distinct  case  when json_extract_scalar(extra, '$.isNewVoice') = 'true' then deviceuid  end) as voiceui_uv, count(case  when json_extract_scalar(extra, '$.isNewVoice') = 'true' then deviceuid end) as voiceui_pv from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and opertype = 'item' and item = 'voicekey' and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_3 + ";"),
    # """
    # Kika语音UI使用的次数（分子）
    # # 当前日期减3"
    # """
    (["openvoiceui_uv", "openvoiceui_pv"], "select count(distinct deviceuid) as openvoiceui_uv, count(deviceuid) as openvoiceui_pv from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and opertype = 'item' and item = 'pop_show' and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_3 + ";"),
    # """
    # Kika键盘Google语音数量（分母）# 当前日期减3
    # """
    (["googlevoiceui_uv", "googlevoiceui_pv"], "select count(distinct deviceuid) as googlevoiceui_uv, count(deviceuid) as googlevoiceui_pv from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and not (layout = 'layout_kika_voice_new' and opertype = 'item' and item = 'voicekey') and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_3 + ";"),
    # """
    # Kika键盘Google语音打开数目（分子）当前日期减3
    # """
    (["opengoogleui_uv"], "select count(distinct a.deviceuid) as opengoogleui_uv from (select distinct deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'keyboard' and item = 'voice' and operType = 'item' and lower(lang) = 'en_us' and json_extract_scalar(extra, '$.imiId') = 'com.google.android.googlequicksearchbox/com.google.android.voicesearch.ime.VoiceInputMethodService' and version >= '5.5.8.2359' and cdate = " + cdate_3 + ")a join (select distinct deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and not (layout = 'layout_kika_voice_new' and opertype = 'item' and item = 'voicekey') and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_3 + ")b on a.deviceuid = b.deviceuid ;"),
    # """
    #  Kika语音用户，切换到Google语音的数量（分子）当前日期减3
    # """
    (["changeout_uv"], "select count(distinct a.deviceuid) as changeout_uv from (select distinct deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'keyboard' and item = 'change_out' and lower(lang) = 'en_us' and json_extract_scalar(extra, '$.ime') = 'com.google.android.googlequicksearchbox/com.google.android.voicesearch.ime.VoiceInputMethodService' and version >= '5.5.8.2359' and cdate = " + cdate_3 + ")a join (select distinct deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and json_extract_scalar(extra, '$.isNewVoice') = 'true' and layout = 'layout_kika_voice_new' and item = 'voicekey' and lower(lang) = 'en_us' and operType = 'item' and version >= '5.5.8.2359' and cdate = " + cdate_3 + ")b on a.deviceuid = b.deviceuid;"),
    # """
    # 行为数据：
    # - pause键
    # - start键
    # - delete键
    # - enter键
    # - punctuation键
    # # 当前日期减3
    # """
    (["validvoice_uv", "validvoice_pv",
      "duration_total", "duration_avg", "duration_median",
      "clickpause_uv", "clickpause_pv", "pause_total",
      "clickstart_uv", "clickstart_pv", "start_total",
      "clickdel_uv", "clickdel_pv", "delword_total",
      "clickenter_uv", "clickenter_pv", "enter_total",
      "clickpunc_uv", "clickpunc_pv", "punc_total"], "select count(distinct deviceuid) as validvoice_uv, count(deviceuid) as validvoice_pv, sum(cast(json_extract_scalar(extra, '$.duration') as bigint)) as duration_total, avg(cast(json_extract_scalar(extra, '$.duration') as bigint)) as duration_avg, approx_percentile(cast(json_extract_scalar(extra, '$.duration') as bigint), 0.5) as duration_median, count(distinct case when cast(json_extract_scalar(extra, '$.pause_count') as bigint) >= 1 then deviceuid end) as clickpause_uv, count(case when cast(json_extract_scalar(extra, '$.pause_count') as bigint) >= 1 then deviceuid end) as clickpause_pv, sum(cast(json_extract_scalar(extra, '$.pause_count') as bigint)) as pause_total, count(distinct case when cast(json_extract_scalar(extra, '$.start_count') as bigint) >= 1 then deviceuid end) as clickstart_uv, count(case when cast(json_extract_scalar(extra, '$.start_count') as bigint) >= 1 then deviceuid end) as clickstart_pv, sum(cast(json_extract_scalar(extra, '$.start_count') as bigint)) as start_total, count(distinct case when cast(json_extract_scalar(extra, '$.total_delete_word_num') as bigint) >= 1 then deviceuid end) as clickdel_uv, count(case when cast(json_extract_scalar(extra, '$.total_delete_word_num') as bigint) >= 1 then deviceuid end) as clickdel_pv, sum(cast(json_extract_scalar(extra, '$.total_delete_word_num') as bigint)) as delword_total, count(distinct case when cast(json_extract_scalar(extra, '$.total_enter_count') as bigint) >= 1 then deviceuid end) as clickenter_uv, count(case when cast(json_extract_scalar(extra, '$.total_enter_count') as bigint) >= 1 then deviceuid end) as clickenter_pv, sum(cast(json_extract_scalar(extra, '$.total_enter_count') as bigint)) as enter_total, count(distinct case when cast(json_extract_scalar(extra, '$.total_more_key_count') as bigint) >= 1 then deviceuid end) as clickpunc_uv, count(case when cast(json_extract_scalar(extra, '$.total_more_key_count') as bigint) >= 1 then deviceuid end) as clickpunc_pv, sum(cast(json_extract_scalar(extra, '$.total_more_key_count') as bigint)) as punc_total from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and operType = 'item' and item = 'pop_dismiss' and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_3 + " and cast(json_extract_scalar(extra, '$.duration') as bigint) > 2000;"),
    # """
    # 行为数据：
    # - emoji键
    # # 当前日期减3
    # """
    (["clickemoji_uv", "clickemoji_pv"], "select count(distinct deviceuid) as clickemoji_uv, count(deviceuid) as clickemoji_pv from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and lower(lang) = 'en_us' and operType = 'item' and item = 'click_emoji' and version >= '5.5.8.2359' and cdate = " + cdate_3 + ";"),

    # """
    # 行为数据：
    # - 输入句号数目
    # - 输入  # 数目
    # - 数据逗号数目
    # - 输入问号数目
    # - 输入感叹号数目
    # - 输入 - 数目
    # - 输入 @ 数目
    # - 输入‘数目
    # - 输入：数目
    # # 当前日期减3"
    # """
    (["period_uv", "period_pv", "comma_uv", "comma_pv", "question_uv", "question_pv", "exclaim_uv", "exclaim_pv", "dash_uv",
      "dash_pv", "at_uv", "at_pv", "hashtag_uv", "hashtag_pv", "colon_uv", "colon_pv", "squote_uv", "squote_pv"],
     "select count(distinct case when json_extract_scalar(extra, '$.sym') = '.' then deviceuid end) as period_uv, count(case when json_extract_scalar(extra, '$.sym') = '.' then deviceuid end) as period_pv, count(distinct case when json_extract_scalar(extra, '$.sym') = ',' then deviceuid end) as comma_uv, count(case when json_extract_scalar(extra, '$.sym') = ',' then deviceuid end) as comma_pv, count(distinct case when json_extract_scalar(extra, '$.sym') = '?' then deviceuid end) as question_uv, count(case when json_extract_scalar(extra, '$.sym') = '?' then deviceuid end) as question_pv, count(distinct case when json_extract_scalar(extra, '$.sym') = '!' then deviceuid end) as exclaim_uv, count(case when json_extract_scalar(extra, '$.sym') = '!' then deviceuid end) as exclaim_pv, count(distinct case when json_extract_scalar(extra, '$.sym') = '-' then deviceuid end) as dash_uv, count(case when json_extract_scalar(extra, '$.sym') = '-' then deviceuid end) as dash_pv, count(distinct case when json_extract_scalar(extra, '$.sym') = '@' then deviceuid end) as at_uv, count(case when json_extract_scalar(extra, '$.sym') = '@' then deviceuid end) as at_pv, count(distinct case when json_extract_scalar(extra, '$.sym') = '#' then deviceuid end) as hashtag_uv, count(case when json_extract_scalar(extra, '$.sym') = '#' then deviceuid end) as hashtag_pv, count(distinct case when json_extract_scalar(extra, '$.sym') = ':' then deviceuid end) as colon_uv, count(case when json_extract_scalar(extra, '$.sym') = ':' then deviceuid end) as colon_pv, count(distinct case when json_extract_scalar(extra, '$.sym') = '''' then deviceuid end) as squote_uv, count(case when json_extract_scalar(extra,'$.sym')='''' then deviceuid end) as squote_pv from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and operType = 'item' and item = 'click_more' and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_3 + ";"),

    # """
    # 留存数据：新用户
    # # 当前日期减3
    # # 当前日期减2
    # """
    (["voice_new", "voice_new_ret1d"], "select count(distinct a.deviceuid) as voice_new, count(distinct d.deviceuid) as voice_new_ret1d from ((select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.dnu where app_key = '78472ddd7528bcacc15725a16aeec190' and lower(lang) = 'en_us' and lower(nation) = 'us' and cdate = " + cdate_3 + ")a join (select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and operType = 'item' and item = 'pop_dismiss' and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_3 + " and cast( json_extract_scalar(extra, '$.duration') as bigint) > 2000)b on a.deviceuid = b.deviceuid) left join (select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and operType = 'item' and item = 'pop_dismiss' and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_2 + " and cast( json_extract_scalar(extra, '$.duration') as bigint) > 2000)d on a.deviceuid = d.deviceuid "),

    # """
    # 留存数据：已有用户
    # # 当前日期减3
    # # 当前日期减2
    # """
    (["voice_old", "voice_old_ret1d"], "select count(distinct b.deviceuid) as voice_old, count(distinct d.deviceuid) as voice_old_ret1d from (select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and operType = 'item' and item = 'pop_dismiss' and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_3 + " and cast( json_extract_scalar(extra, '$.duration') as bigint) > 2000)b left join (select case when aid is not null then aid else deviceuid end as deviceuid from gimbal.event where app_key = '78472ddd7528bcacc15725a16aeec190' and layout = 'layout_kika_voice_new' and operType = 'item' and item = 'pop_dismiss' and lower(lang) = 'en_us' and version >= '5.5.8.2359' and cdate = " + cdate_2 + " and cast( json_extract_scalar(extra, '$.duration') as bigint) > 2000)d on b.deviceuid = d.deviceuid;")
    ]

    cursor = db.cursor()
    insert_sql_dict_list = {}
    for item in sql_list:
        result = []
        count = 3
        while count > 0:
            count -= 1
            keys, sql = item
            print(keys, athena_shell + "\"" + sql + "\"")
            p = subprocess.Popen(athena_shell + "\"" + sql + "\"", shell=True, stdout=subprocess.PIPE)
            subprocess.Popen.wait(p)
            result = p.stdout.readlines()
            count_key = len(keys)
            print("result: " + str(result))
            if len(result) > 0:
                for line in result:
                    line = line.decode()
                    line = line.replace("\n", "")
                    valuses = line.split("\t")
                    if count_key == len(valuses):
                        for i in range(count_key):
                            insert_sql_dict_list[keys[i]] = valuses[i]
                break
            time.sleep(5)
    # "`create_date` = '" + cdate_start + "' and `app_key` = " + k1
    insert_sql = "INSERT INTO `voice_daily_v2` (`create_date`, `app_key`, `"
    for key_name in insert_sql_dict_list.keys():
        insert_sql = insert_sql + key_name + "`,`"
    insert_sql = insert_sql[:-2] + ") VALUES "
    val_sql = "('" + cdate_3 + "', " + k1 + ", '"
    for val in insert_sql_dict_list.values():
        val_sql = val_sql + val + "','"
    val_sql = val_sql[:-2] + ")"
    insert_sql = insert_sql + val_sql
    print("insert: sql: " + insert_sql)
    my_sql = insert_sql

    #
    # Execute SQL
    try:
        cursor.execute(my_sql)
        db.commit()
    except:
        db.rollback()


if __name__ == '__main__':
    # test_voice_default_date()
    # test_voice_update_data_before()
    for i in range(7):
        cdate_3 = get_cdate(3 + i)
        cdate_2 = get_cdate(2 + i)
        test_voice_daily_v2()
