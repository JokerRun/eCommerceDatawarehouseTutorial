insert into table dws_new_mid_day
select ud.mid_id,
       ud.user_id,
       ud.version_code,
       ud.version_name,
       ud.lang,
       ud.source,
       ud.os,
       ud.area,
       ud.model,
       ud.brand,
       ud.sdk_version,
       ud.gmail,
       ud.height_width,
       ud.app_time,
       ud.network,
       ud.lng,
       ud.lat,
       '2019-02-10'
from dws_uv_detail_day ud
         left join dws_new_mid_day nm on ud.mid_id = nm.mid_id
where ud.dt = '2019-02-10'
  and nm.mid_id is null;

insert into table ads_new_mid_count
select create_date,
       count(*)
from dws_new_mid_day
where create_date = '2019-02-10'
group by create_date;