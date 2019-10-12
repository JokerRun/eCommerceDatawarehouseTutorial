#!/bin/bash

# 定义变量方便修改
APP=gmall
hive=/usr/bin/hive
# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$1" ] ;then
    do_date=$1
else 
    do_date=`date -d "-1 day" +%F`  
fi 

sql="
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table $APP.dwd_start_log
PARTITION (dt='$do_date')
SELECT 
    get_json_object(arr[1],'$.cm.mid') mid_id,
    get_json_object(arr[1],'$.cm.uid') user_id,
    get_json_object(arr[1],'$.cm.vc') version_code,
    get_json_object(arr[1],'$.cm.vn') version_name,
    get_json_object(arr[1],'$.cm.l') lang,
    get_json_object(arr[1],'$.cm.sr') source,
    get_json_object(arr[1],'$.cm.os') os,
    get_json_object(arr[1],'$.cm.ar') area,
    get_json_object(arr[1],'$.cm.md') model,
    get_json_object(arr[1],'$.cm.ba') brand,
    get_json_object(arr[1],'$.cm.sv') sdk_version,
    get_json_object(arr[1],'$.cm.g') gmail,
    get_json_object(arr[1],'$.cm.hw') height_width,
    get_json_object(arr[1],'$.cm.t') app_time,
    get_json_object(arr[1],'$.cm.nw') network,
    get_json_object(arr[1],'$.cm.ln') lng,
    get_json_object(arr[1],'$.cm.la') lat,
    get_json_object(arr[1],'$.et[0].kv.entry') entry,
    get_json_object(arr[1],'$.et[0].kv.open_ad_type') open_ad_type,
    get_json_object(arr[1],'$.et[0].kv.action') action,
    get_json_object(arr[1],'$.et[0].kv.loading_time') loading_time,
    get_json_object(arr[1],'$.et[0].kv.detail') detail,
    get_json_object(arr[1],'$.et[0].ett') extend1
FROM
  (
   SELECT split(line,'\\\|') arr
   from $APP.ods_start_log 
   where dt='$do_date'
  ) tmp
"
echo "$sql"
$hive -e "$sql"
