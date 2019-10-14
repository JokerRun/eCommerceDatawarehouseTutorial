 set hive.exec.dynamic.partition.mode=nonstrict;

  insert overwrite table gmall.dws_uv_detail_day partition(dt='${load_date}')
  select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang)) lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat
  from gmall.dwd_start_log
  where dt='${load_date}'
  group by mid_id;


  insert overwrite table gmall.dws_uv_detail_wk partition(wk_dt)
  select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang)) lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat,
    date_add(next_day('${load_date}','MO'),-7),
    date_add(next_day('${load_date}','MO'),-1),
    concat(date_add( next_day('${load_date}','MO'),-7), '_' , date_add(next_day('${load_date}','MO'),-1)
  )
  from gmall.dws_uv_detail_day
  where dt>=date_add(next_day('${load_date}','MO'),-7) and dt<=date_add(next_day('${load_date}','MO'),-1)
  group by mid_id;


  insert overwrite table gmall.dws_uv_detail_mn partition(mn)
  select
    mid_id,
    concat_ws('|', collect_set(user_id)) user_id,
    concat_ws('|', collect_set(version_code)) version_code,
    concat_ws('|', collect_set(version_name)) version_name,
    concat_ws('|', collect_set(lang))lang,
    concat_ws('|', collect_set(source)) source,
    concat_ws('|', collect_set(os)) os,
    concat_ws('|', collect_set(area)) area,
    concat_ws('|', collect_set(model)) model,
    concat_ws('|', collect_set(brand)) brand,
    concat_ws('|', collect_set(sdk_version)) sdk_version,
    concat_ws('|', collect_set(gmail)) gmail,
    concat_ws('|', collect_set(height_width)) height_width,
    concat_ws('|', collect_set(app_time)) app_time,
    concat_ws('|', collect_set(network)) network,
    concat_ws('|', collect_set(lng)) lng,
    concat_ws('|', collect_set(lat)) lat,
    date_format('${load_date}','yyyy-MM')
  from gmall.dws_uv_detail_day
  where date_format(dt,'yyyy-MM') = date_format('${load_date}','yyyy-MM')
  group by mid_id;
  
  
insert into table gmall.ads_uv_count 
select  
  '${load_date}' dt,
   daycount.ct,
   wkcount.ct,
   mncount.ct,
   if(date_add(next_day('${load_date}','MO'),-1)='${load_date}','Y','N') ,
   if(last_day('${load_date}')='${load_date}','Y','N') 
from 
(
   select  
      '${load_date}' dt,
       count(*) ct
   from gmall.dws_uv_detail_day
   where dt='${load_date}'  
)daycount   join 
( 
   select  
     '${load_date}' dt,
     count (*) ct
   from gmall.dws_uv_detail_wk
   where wk_dt=concat(date_add(next_day('${load_date}','MO'),-7),'_' ,date_add(next_day('${load_date}','MO'),-1) )
)  wkcount  on daycount.dt=wkcount.dt
join 
( 
   select  
     '${load_date}' dt,
     count (*) ct
   from gmall.dws_uv_detail_mn
   where mn=date_format('${load_date}','yyyy-MM')  
)mncount on daycount.dt=mncount.dt;