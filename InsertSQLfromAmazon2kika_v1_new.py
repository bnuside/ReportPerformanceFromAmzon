#!/usr/bin/python
# -*- coding: UTF-8 -*-

import subprocess
import pymysql
import time
import datetime


def get_cdate(i):
    time_today = time.localtime(time.time())
    year = time_today.tm_year
    mon = time_today.tm_mon
    day = time_today.tm_mday
    # year = 2017
    # mon = 7
    # day = 25
    return str(datetime.datetime(year, mon, day) - datetime.timedelta(i)).split()[0].replace('-', '')


time_now = time.localtime(time.time())
cdate_2 = get_cdate(2)
cdate_3 = get_cdate(3)
cdate_4 = get_cdate(4)
cdate_8 = get_cdate(8)
cdate_16 = get_cdate(16)
cdate_31 = get_cdate(31)

# cdate_2 = "20170630"
# cdate_3 = "20170629"

athena_shell = "/usr/local/jdk1.8.0_51/bin/java -classpath /home/pubsrv/project/athenaaccess/libs/athena_query_kika_tool-1.0.jar:" \
      "/home/pubsrv/project/athenaaccess/libs/AthenaJDBC41-1.0.0.jar:" \
      "/home/pubsrv/project/athenaaccess/libs/mysql-connector-java-5.1.40-bin.jar" \
      " com.kika.tech.athena_query_kika_tool.AthenaQueryTool" \
      " AKIAIUO2VW53QUXXMCFQ "

k1='\'78472ddd7528bcacc15725a16aeec190\''
k2='\'4e5ab3a6d2140457e0423a28a094b1fd\''
k3='\'e2934742f9d3b8ef2b59806a041ab389\''

sql_list = [
    # k3
    # dau_organic
    (["dau_organic", "app_key", "create_date"],"SELECT count(distinct case when aid is not null then aid else deviceuid end) AS dau_organic,app_key,cdate FROM gimbal.service_merge_daily WHERE app_key IN (" + k3 + ") AND cdate=" + cdate_2 + " AND (iskeyboard=1 or isapp=1) GROUP BY  app_key,cdate; "),
    # dnu_organic
    (["dnu_organic", "app_key", "create_date"], "select count(distinct deviceuid) as dnu_organic,app_key,cdate from gimbal.dnu where app_key in (" + k3 + ") and cdate=" + cdate_2 + " group by app_key,cdate;"),
    # dnu_us_organic
    (["dnu_us_organic", "app_key", "create_date"], "select count(distinct deviceuid) as dnu_us_organic,app_key,cdate from gimbal.dnu  where app_key in (" + k3 + ") and nation='us' and cdate=" + cdate_2 + " group by app_key,cdate;"),
    # setup_finish_rate
    (["setup_finish_rate_setup_step1","setup_finish_rate_setup_step_finish", "app_key","create_date"], "SELECT count(distinct c.deviceuid) AS setup_finish_rate_setup_step1, count(distinct d.deviceuid) AS setup_finish_rate_setup_step_finish, c.app_key, cdate FROM  (SELECT DISTINCT a.deviceuid, a.app_key, cdate FROM  (SELECT DISTINCT deviceuid, app_key, cdate FROM gimbal.event WHERE app_key IN (" + k3 + ") AND layout='setup_step1' AND item='show'  AND cdate=" + cdate_2 + " AND event_part='other')a JOIN  (SELECT DISTINCT deviceuid, app_key  FROM gimbal.dnu WHERE app_key IN (" + k3 + ") AND cdate=" + cdate_2 + ")b ON a.deviceuid=b.deviceuid AND a.app_key=b.app_key)c left outer JOIN  (SELECT DISTINCT deviceuid FROM gimbal.event WHERE app_key IN (" + k3 + ") AND layout='setup_step' AND item='finish' AND cdate=" + cdate_2 + " AND event_part='other')d ON c.deviceuid=d.deviceuid AND c.app_key=c.app_key GROUP BY  c.app_key,cdate;"),
    # setup_finish_rate_setup_step1_us, setup_finish_rate_setup_step_finish_us
    (["setup_finish_rate_setup_step1_us","setup_finish_rate_setup_step_finish_us","app_key","create_date"], "select count(distinct c.deviceuid) as setup_finish_rate_setup_step1_us,count(distinct d.deviceuid) as setup_finish_rate_setup_step_finish_us,c.app_key,cdate from (select distinct a.deviceuid,a.app_key,cdate from (select distinct deviceuid,app_key,cdate from gimbal.event where app_key in (" + k3 + ") and layout='setup_step1' and item='show' and nation='us' and cdate=" + cdate_2 + " and event_part='other')a join (select distinct deviceuid,app_key from gimbal.dnu where app_key in (" + k3 + ") and nation='us' and cdate=" + cdate_2 + ")b on a.deviceuid=b.deviceuid and a.app_key=b.app_key)c left outer join (select distinct deviceuid from gimbal.event where app_key in (" + k3 + ") and layout='setup_step' and item='finish' and nation='us' and cdate=" + cdate_2 + " and event_part='other')d on c.deviceuid=d.deviceuid and c.app_key=c.app_key group by c.app_key,cdate;"),
    # ＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝需要用cdate-3 和cdate-2，表a中的cdate是cdate-3，表b中的cdate是cdate-2
    # keyboard_retention_rate_organic
    (["keyboard_retention_rate_dnu_organic", "keyboard_retention_rate_dau_organic", "app_key","create_date"], "SELECT count(distinct a.deviceuid) AS keyboard_retention_rate_dnu_organic,count(distinct b.deviceuid) AS keyboard_retention_rate_dau_organic,a.app_key,cdate FROM (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid,app_key,cdate FROM gimbal.dnu WHERE app_key IN (" + k3 + ") AND cdate=" + cdate_3 + ")a left outer JOIN (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid,app_key FROM gimbal.service_merge_daily WHERE app_key IN (" + k3 + ") AND cdate=" + cdate_2 + " AND iskeyboard=1)b ON a.deviceuid=b.deviceuid AND a.app_key=b.app_key GROUP BY  a.app_key,cdate;"),
     # keyboard_retention_rate_us_organic
    (["keyboard_retention_rate_us_dnu_organic", "keyboard_retention_rate_us_dau_organic", "app_key","create_date"], "SELECT count(distinct a.deviceuid) AS keyboard_retention_rate_us_dnu_organic,count(distinct b.deviceuid) AS keyboard_retention_rate_us_dau_organic,a.app_key,cdate FROM (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid,app_key,cdate FROM gimbal.dnu WHERE app_key IN (" + k3 + ") and nation='us' AND cdate=" + cdate_3 + ")a left outer JOIN (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid,app_key FROM gimbal.service_merge_daily WHERE app_key IN (" + k3 + ") and nation='us' AND cdate=" + cdate_2 + " AND iskeyboard=1)b ON a.deviceuid=b.deviceuid AND a.app_key=b.app_key GROUP BY  a.app_key,cdate;"),
    # k1,k2
    # dau_us_organic
    (["dau_organic", "app_key", "create_date"], "SELECT count(distinct case when aid is not null then aid else deviceuid end) AS dau_organic,app_key,cdate FROM gimbal.service_merge_daily WHERE app_key IN (" + k1 + "," + k2 + ") AND cdate=" + cdate_2 + " AND (iskeyboard=1 or isapp=1) AND (lower(media_source)='organic' or media_source is null ) GROUP BY  app_key,cdate;"),
    # dnu_organic
    (["dnu_organic", "app_key", "create_date"], "select count(distinct deviceuid) as dnu_organic,app_key,cdate from gimbal.dnu_kv  where app_key in (" + k1 + ") and cdate=" + cdate_2 + " and media_source='organic' group by app_key,cdate;"),
     # dnu_us_organic
    (["dnu_us_organic","app_key","create_date"], "select count(distinct deviceuid) as dnu_us_organic,app_key,cdate from gimbal.dnu_kv where app_key in (" + k1 + ") and cdate=" + cdate_2 + " and media_source='organic' and nation='us' group by app_key,cdate;"),
    # setup_finish_rate
    (["setup_finish_rate_setup_step1","setup_finish_rate_setup_step_finish", "app_key","create_date"], "select count(distinct c.deviceuid) as setup_finish_rate_setup_step1,count(distinct d.deviceuid) as setup_finish_rate_setup_step_finish,c.app_key,cdate from  (select distinct a.deviceuid,a.app_key,cdate from (select distinct deviceuid,app_key,cdate from gimbal.event where app_key in (" + k1 + ") and layout='setup_step1' and item='show' and cdate=" + cdate_2 + " and event_part='other')a join (select distinct deviceuid,app_key from gimbal.dnu_kv  where app_key in (" + k1 + ") and media_source='organic' and cdate=" + cdate_2 + ")b on a.deviceuid=b.deviceuid and a.app_key=b.app_key)c left outer join (select distinct deviceuid from gimbal.event where app_key in (" + k1 + ") and layout='setup_step' and item='finish' and cdate=" + cdate_2 + " and event_part='other')d on c.deviceuid=d.deviceuid and c.app_key=c.app_key group by c.app_key,cdate;"),
    # setup_finish_rate_setup_step1_us, setup_finish_rate_setup_step_finish_us
    (["setup_finish_rate_setup_step1_us","setup_finish_rate_setup_step_finish_us","app_key","create_date"], "select count(distinct c.deviceuid) as setup_finish_rate_setup_step1_us,count(distinct d.deviceuid) as setup_finish_rate_setup_step_finish_us,c.app_key,cdate from (select distinct a.deviceuid,a.app_key,cdate from (select distinct deviceuid,app_key,cdate from gimbal.event where app_key in (" + k1 + ") and layout='setup_step1' and item='show' and nation='us' and cdate=" + cdate_2 + " and event_part='other')a join (select distinct deviceuid,app_key from gimbal.dnu_kv where app_key in (" + k1 + ") and media_source='organic' and nation='us' and cdate=" + cdate_2 + ")b on a.deviceuid=b.deviceuid and a.app_key=b.app_key)c left outer join (select distinct deviceuid from gimbal.event where app_key in (" + k1 + ") and layout='setup_step' and item='finish' and nation='us' and cdate=" + cdate_2 + " and event_part='other')d on c.deviceuid=d.deviceuid and c.app_key=c.app_key group by c.app_key,cdate;"),
     # ＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝需要用cdate-3 和cdate-2，表a中的cdate是cdate-3，表b中的cdate是cdate-2
    # keyboard_retention_rate_organic
    (["keyboard_retention_rate_dnu_organic", "keyboard_retention_rate_dau_organic", "app_key","create_date"], "SELECT count(distinct a.deviceuid) AS keyboard_retention_rate_dnu_organic,count(distinct b.deviceuid) AS keyboard_retention_rate_dau_organic,a.app_key,cdate FROM (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid, app_key, cdate FROM gimbal.dnu_kv WHERE app_key IN (" + k1 + ") AND cdate=" + cdate_3 + " AND media_source='organic')a left outer JOIN  (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid, app_key FROM gimbal.service_merge_daily WHERE app_key IN (" + k1 + ") AND cdate=" + cdate_2 + " AND iskeyboard=1)b ON a.deviceuid=b.deviceuid AND a.app_key=b.app_key GROUP BY  a.app_key,cdate;"),
     # keyboard_retention_rate_us_organic
    (["keyboard_retention_rate_us_dnu_organic", "keyboard_retention_rate_us_dau_organic", "app_key","create_date"], "SELECT count(distinct a.deviceuid) AS keyboard_retention_rate_us_dnu_organic,count(distinct b.deviceuid) AS keyboard_retention_rate_us_dau_organic,a.app_key,cdate FROM (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid,app_key,cdate FROM gimbal.dnu_kv WHERE app_key IN (" + k1 + ") and nation='us' AND cdate=" + cdate_3 + " AND media_source='organic')a left outer JOIN (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid,app_key FROM gimbal.service_merge_daily WHERE app_key IN (" + k1 + ") and nation='us' AND cdate=" + cdate_2 + " AND iskeyboard=1)b ON a.deviceuid=b.deviceuid AND a.app_key=b.app_key GROUP BY  a.app_key,cdate;"),
    # dnu_organic
    (["dnu_organic", "app_key", "create_date"], "select count(distinct deviceuid) as dnu_organic,app_key,cdate from gimbal.dnu_kv  where app_key in (" + k2 + ") and cdate=" + cdate_2 + " and media_source='organic' group by app_key,cdate;"),
     # dnu_us_organic
    (["dnu_us_organic","app_key","create_date"], "select count(distinct deviceuid) as dnu_us_organic,app_key,cdate from gimbal.dnu_kv where app_key in (" + k2 + ") and cdate=" + cdate_2 + " and media_source='organic' and nation='us' group by app_key,cdate;"),
    # setup_finish_rate
    (["setup_finish_rate_setup_step1","setup_finish_rate_setup_step_finish", "app_key","create_date"], "select count(distinct c.deviceuid) as setup_finish_rate_setup_step1,count(distinct d.deviceuid) as setup_finish_rate_setup_step_finish,c.app_key,cdate from  (select distinct a.deviceuid,a.app_key,cdate from (select distinct deviceuid,app_key,cdate from gimbal.event where app_key in (" + k2 + ") and layout='setup_step1' and item='show' and cdate=" + cdate_2 + " and event_part='other')a join (select distinct deviceuid,app_key from gimbal.dnu_kv  where app_key in (" + k2 + ") and media_source='organic' and cdate=" + cdate_2 + ")b on a.deviceuid=b.deviceuid and a.app_key=b.app_key)c left outer join (select distinct deviceuid from gimbal.event where app_key in (" + k2 + ") and layout='setup_step' and item='finish' and cdate=" + cdate_2 + " and event_part='other')d on c.deviceuid=d.deviceuid and c.app_key=c.app_key group by c.app_key,cdate;"),
    # setup_finish_rate_setup_step1_us, setup_finish_rate_setup_step_finish_us
    (["setup_finish_rate_setup_step1_us","setup_finish_rate_setup_step_finish_us","app_key","create_date"], "select count(distinct c.deviceuid) as setup_finish_rate_setup_step1_us,count(distinct d.deviceuid) as setup_finish_rate_setup_step_finish_us,c.app_key,cdate from (select distinct a.deviceuid,a.app_key,cdate from (select distinct deviceuid,app_key,cdate from gimbal.event where app_key in (" + k2 + ") and layout='setup_step1' and item='show' and nation='us' and cdate=" + cdate_2 + " and event_part='other')a join (select distinct deviceuid,app_key from gimbal.dnu_kv where app_key in (" + k2 + ") and media_source='organic' and nation='us' and cdate=" + cdate_2 + ")b on a.deviceuid=b.deviceuid and a.app_key=b.app_key)c left outer join (select distinct deviceuid from gimbal.event where app_key in (" + k2 + ") and layout='setup_step' and item='finish' and nation='us' and cdate=" + cdate_2 + " and event_part='other')d on c.deviceuid=d.deviceuid and c.app_key=c.app_key group by c.app_key,cdate;"),
     # ＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝＝需要用cdate-3 和cdate-2，表a中的cdate是cdate-3，表b中的cdate是cdate-2
    # keyboard_retention_rate_organic
    (["keyboard_retention_rate_dnu_organic", "keyboard_retention_rate_dau_organic", "app_key","create_date"], "SELECT count(distinct a.deviceuid) AS keyboard_retention_rate_dnu_organic,count(distinct b.deviceuid) AS keyboard_retention_rate_dau_organic,a.app_key,cdate FROM (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid, app_key, cdate FROM gimbal.dnu_kv WHERE app_key IN (" + k2 + ") AND cdate=" + cdate_3 + " AND media_source='organic')a left outer JOIN  (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid, app_key FROM gimbal.service_merge_daily WHERE app_key IN (" + k2 + ") AND cdate=" + cdate_2 + " AND iskeyboard=1)b ON a.deviceuid=b.deviceuid AND a.app_key=b.app_key GROUP BY  a.app_key,cdate;"),
     # keyboard_retention_rate_us_organic
    (["keyboard_retention_rate_us_dnu_organic", "keyboard_retention_rate_us_dau_organic", "app_key","create_date"], "SELECT count(distinct a.deviceuid) AS keyboard_retention_rate_us_dnu_organic,count(distinct b.deviceuid) AS keyboard_retention_rate_us_dau_organic,a.app_key,cdate FROM (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid,app_key,cdate FROM gimbal.dnu_kv WHERE app_key IN (" + k2 + ") and nation='us' AND cdate=" + cdate_3 + " AND media_source='organic')a left outer JOIN (SELECT DISTINCT (case when aid is not null then aid else deviceuid end) as deviceuid,app_key FROM gimbal.service_merge_daily WHERE app_key IN (" + k2 + ") and nation='us' AND cdate=" + cdate_2 + " AND iskeyboard=1)b ON a.deviceuid=b.deviceuid AND a.app_key=b.app_key GROUP BY  a.app_key,cdate;"),
    # k1,k2,k3
    # dau_organic
    # dau_us_organic
    (["dau_us_organic","app_key","create_date"], "SELECT count(distinct case when aid is not null then aid else deviceuid end) AS dau_us_organic,app_key,cdate FROM gimbal.service_merge_daily WHERE app_key IN ("+k1+","+k2+") AND cdate="+cdate_2+" AND (iskeyboard=1 or isapp=1) AND (media_source='organic' or media_source is null ) and nation='us' GROUP BY  app_key,cdate;"),
    # input_times_per_person
    (["input_times_per_person_dlu","input_times_per_person_inputtime", "app_key","create_date"], "select count(distinct deviceuid) as input_times_per_person_dlu,count(*) as input_times_per_person_inputtime,app_key,cdate from gimbal.word_trace_desensitization   where app_key in (" + k1 + ") and cdate=" + cdate_2 + " group by app_key,cdate;"),
    # emoji_input_time_per_person
    (["input_times_per_person_dlu","emoji_input_times_per_person_inputtime","app_key","create_date"], "select count(distinct deviceuid) as input_times_per_person_dlu,sum(emoji_count) as emoji_input_times_per_person_inputtime,app_key,cdate from gimbal.word_trace_desensitization where app_key in (" + k1 + ") and cdate=" + cdate_2 + " group by app_key,cdate;"),
    # emoji_entry_times_per_person
    (["emoji_entry_times_per_person_dlu", "emoji_entry_times_per_person_emoji_entry_times", "app_key", "create_date"], "select count(distinct a.deviceuid) as emoji_entry_times_per_person_dlu,count (b.deviceuid) as emoji_entry_times_per_person_emoji_entry_times,a.app_key,cdate from (select distinct deviceuid,app_key,cdate from gimbal.service_merge_daily where app_key in (" + k1 + ") and cdate=" + cdate_2 + " and iskeyboard=1)a left outer join (select deviceuid,app_key from gimbal.event where app_key in (" + k1 + ") and cdate=" + cdate_2 + "  and layout='keyboard_emoji' and item='show' and event_part='keyboard')b on a.deviceuid=b.deviceuid and a.app_key=b.app_key group by a.app_key,cdate;"),
    # input_times_per_person
    (["input_times_per_person_dlu","input_times_per_person_inputtime", "app_key","create_date"], "select count(distinct deviceuid) as input_times_per_person_dlu,count(*) as input_times_per_person_inputtime,app_key,cdate from gimbal.word_trace_desensitization   where app_key in (" + k2 + ") and cdate=" + cdate_2 + " group by app_key,cdate;"),
    # emoji_input_time_per_person
    (["input_times_per_person_dlu","emoji_input_times_per_person_inputtime","app_key","create_date"], "select count(distinct deviceuid) as input_times_per_person_dlu,sum(emoji_count) as emoji_input_times_per_person_inputtime,app_key,cdate from gimbal.word_trace_desensitization where app_key in (" + k2 + ") and cdate=" + cdate_2 + " group by app_key,cdate;"),
    # emoji_entry_times_per_person
    (["emoji_entry_times_per_person_dlu", "emoji_entry_times_per_person_emoji_entry_times", "app_key", "create_date"], "select count(distinct a.deviceuid) as emoji_entry_times_per_person_dlu,count (b.deviceuid) as emoji_entry_times_per_person_emoji_entry_times,a.app_key,cdate from (select distinct deviceuid,app_key,cdate from gimbal.service_merge_daily where app_key in (" + k2 + ") and cdate=" + cdate_2 + " and iskeyboard=1)a left outer join (select deviceuid,app_key from gimbal.event where app_key in (" + k2 + ") and cdate=" + cdate_2 + "  and layout='keyboard_emoji' and item='show' and event_part='keyboard')b on a.deviceuid=b.deviceuid and a.app_key=b.app_key group by a.app_key,cdate;"),
    # dau_us_organic
    (["dau_us_organic","app_key","create_date"], "SELECT count(distinct case when aid is not null then aid else deviceuid end) AS dau_us_organic,app_key,cdate FROM gimbal.service_merge_daily WHERE app_key IN ("+k3+") AND cdate="+cdate_2+" AND (iskeyboard=1 or isapp=1) and nation='us' GROUP BY app_key,cdate;"),
    # input_times_per_person
    (["input_times_per_person_dlu","input_times_per_person_inputtime", "app_key","create_date"], "select count(distinct deviceuid) as input_times_per_person_dlu,count(*) as input_times_per_person_inputtime,app_key,cdate from gimbal.word_trace_desensitization   where app_key in (" + k3 + ") and cdate=" + cdate_2 + " group by app_key,cdate;"),
    # emoji_input_time_per_person
    (["input_times_per_person_dlu","emoji_input_times_per_person_inputtime","app_key","create_date"], "select count(distinct deviceuid) as input_times_per_person_dlu,sum(emoji_count) as emoji_input_times_per_person_inputtime,app_key,cdate from gimbal.word_trace_desensitization where app_key in (" + k3 + ") and cdate=" + cdate_2 + " group by app_key,cdate;"),
    # emoji_entry_times_per_person
    (["emoji_entry_times_per_person_dlu", "emoji_entry_times_per_person_emoji_entry_times", "app_key", "create_date"], "select count(distinct a.deviceuid) as emoji_entry_times_per_person_dlu,count (b.deviceuid) as emoji_entry_times_per_person_emoji_entry_times,a.app_key,cdate from (select distinct deviceuid,app_key,cdate from gimbal.service_merge_daily where app_key in (" + k3 + ") and cdate=" + cdate_2 + " and iskeyboard=1)a left outer join (select deviceuid,app_key from gimbal.event where app_key in (" + k3 + ") and cdate=" + cdate_2 + "  and layout='keyboard_emoji' and item='show' and event_part='keyboard')b on a.deviceuid=b.deviceuid and a.app_key=b.app_key group by a.app_key,cdate;"),

#新增部分
    # keyboard_onscreen_times_per_person_k3
    (["keyboard_onscreen_times_per_person_dlu", "keyboard_onscreen_times_per_person_core_count_keyboard_popup", "app_key","create_date"], "select count(distinct deviceuid) as keyboard_onscreen_times_per_person_dlu,sum(cast (json_extract_scalar(extra,'$.show_time') as bigint)) as keyboard_onscreen_times_per_person_core_count_keyboard_popup,app_key,cdate from gimbal.event where app_key in (" + k3 + ") and layout='keyboard' and item='show_duration' and cdate=" + cdate_2 + " and event_part='keyboard' group by app_key,cdate;"),
    # keyboard_onscreen_times_per_person_k2
    (["keyboard_onscreen_times_per_person_dlu", "keyboard_onscreen_times_per_person_core_count_keyboard_popup", "app_key","create_date"], "select count(distinct deviceuid) as keyboard_onscreen_times_per_person_dlu,sum(cast (json_extract_scalar(extra,'$.show_time') as bigint)) as keyboard_onscreen_times_per_person_core_count_keyboard_popup,app_key,cdate from gimbal.event where app_key in (" + k2 + ") and layout='keyboard' and item='show_duration' and cdate=" + cdate_2 + " and event_part='keyboard' group by app_key,cdate;"),
    # keyboard_onscreen_times_per_person_k1
    (["keyboard_onscreen_times_per_person_dlu", "keyboard_onscreen_times_per_person_core_count_keyboard_popup", "app_key","create_date"], "select count(distinct deviceuid) as keyboard_onscreen_times_per_person_dlu,sum(cast (json_extract_scalar(extra,'$.show_time') as bigint)) as keyboard_onscreen_times_per_person_core_count_keyboard_popup,app_key,cdate from gimbal.event where app_key in (" + k1 + ") and layout='keyboard' and item='show_duration' and cdate=" + cdate_2 + " and event_part='keyboard' group by app_key,cdate;"),
    # keyboard_onscreen_times_per_person_us_k3
    (["keyboard_onscreen_times_per_person_dlu_us", "keyboard_onscreen_times_per_person_core_count_keyboard_popup_us", "app_key","create_date"], "select count(distinct deviceuid) as keyboard_onscreen_times_per_person_dlu_us,sum(cast (json_extract_scalar(extra,'$.show_time') as bigint)) as keyboard_onscreen_times_per_person_core_count_keyboard_popup_us,app_key,cdate from gimbal.event where app_key in (" + k3 + ") and layout='keyboard' and item='show_duration' and cdate=" + cdate_2 + " and nation='us' and event_part='keyboard' group by app_key,cdate;"),
     # keyboard_onscreen_times_per_person_us_k2
    (["keyboard_onscreen_times_per_person_dlu_us", "keyboard_onscreen_times_per_person_core_count_keyboard_popup_us", "app_key","create_date"], "select count(distinct deviceuid) as keyboard_onscreen_times_per_person_dlu_us,sum(cast (json_extract_scalar(extra,'$.show_time') as bigint)) as keyboard_onscreen_times_per_person_core_count_keyboard_popup_us,app_key,cdate from gimbal.event where app_key in (" + k2 + ") and layout='keyboard' and item='show_duration' and cdate=" + cdate_2 + " and nation='us' and event_part='keyboard' group by app_key,cdate;"),
     # keyboard_onscreen_times_per_person_us_k1
    (["keyboard_onscreen_times_per_person_dlu_us", "keyboard_onscreen_times_per_person_core_count_keyboard_popup_us", "app_key","create_date"], "select count(distinct deviceuid) as keyboard_onscreen_times_per_person_dlu_us,sum(cast (json_extract_scalar(extra,'$.show_time') as bigint)) as keyboard_onscreen_times_per_person_core_count_keyboard_popup_us,app_key,cdate from gimbal.event where app_key in (" + k1 + ") and layout='keyboard' and item='show_duration' and cdate=" + cdate_2 + " and nation='us' and event_part='keyboard' group by app_key,cdate;"),
   
]


"""
KeyboardProduct

有两种方式将指标入库
1.    程序
       MySQL地址为：
       AWS内网IP：172.31.21.163
       db_nme: kika_key_metric
       TableName: keyboard_product
       user:  productuser
       pwd:  dCv_PcDswjGe

       可读可写可删可以更改表结构
"""
db = pymysql.connect(host="172.31.21.163", user="productuser", passwd="dCv_PcDswjGe", db="kika_key_metrics")

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
                if cdate_2 in line or cdate_3 in line:
                    line = line.replace("\n", "")
                    valuses = line.split("\t")
                    if count_key == len(valuses):
                        temp_sql_dict = {}
                        for i in range(count_key):
                            temp_sql_dict[keys[i]] = valuses[i]
                        insert_sql_name = temp_sql_dict["app_key"] + temp_sql_dict["create_date"]
                        if insert_sql_name in insert_sql_dict_list:
                            for key, val in temp_sql_dict.items():
                                insert_sql_dict_list[insert_sql_name][key] = val
                        else:
                            insert_sql_dict_list[insert_sql_name] = temp_sql_dict
            break
        time.sleep(5)

for name, sql_value in insert_sql_dict_list.items():
    if sql_value["create_date"] == cdate_2:
        insert_sql = "INSERT INTO `keyboard_product` (`"
        for key_name in sql_value.keys():
            insert_sql = insert_sql + key_name + "`,`"
        insert_sql = insert_sql[:-2] + ") VALUES "
        val_sql = "('"
        for val in sql_value.values():
            val_sql = val_sql + val + "','"
        val_sql = val_sql[:-2] + ")"
        insert_sql = insert_sql + val_sql
        print("insert: sql: " + insert_sql)
        my_sql = insert_sql
    else:
        update_sql = "UPDATE `keyboard_product` SET "
        for key, val in sql_value.items():
            update_sql = update_sql + "`" + str(key) + "` = '" + str(val) + "',"
        update_sql = update_sql[:-1] + " WHERE "
        update_sql += "`create_date` = '" + cdate_3 + "' and `app_key` = '" + sql_value["app_key"] + "'"
        my_sql = update_sql
        print("Update: sql: " + update_sql)

    #
    # Execute SQL
    try:
        cursor.execute(my_sql)
        db.commit()
    except:
        db.rollback()


# # SQL for Insert tables
#
#
#
# # Execute SQL
# try:
#     cursor.execute(sql)
#     db.commit()
# except:
#     db.rollback()


