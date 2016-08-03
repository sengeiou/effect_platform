-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2012-10-22
-- # ------------------------------------------------------------------------
-- # Description: 无线一阳指生成活动维表
-- # Input: wireless_dim_yyz_page
-- # Onput: lz_dim_ep_wireless_yyz
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################
import udf:date_add;
import udf:date_diff;
import udf:trunc;
set hive.merge.mapredfiles=true;
insert overwrite table lz_dim_ep_wireless_yyz partition(dt='${date}') 
select
    id as plan_id,
    get_json_object(page_attribute, '$.title') as title,
    get_json_object(page_attribute, '$.finalUrl') as url,
    creater,
    gmt_modified,
    date_add(gmt_modified, 15) as gmt_end
from 
    wireless_dim_yyz_page 
where 
    ds='${date}' 
    and date_diff(date_add(gmt_modified, 15), trunc('${date}', 'yyyyMMdd', 'yyyy-MM-dd')) >= 0
group by
    id,
    get_json_object(page_attribute, '$.title'),
    get_json_object(page_attribute, '$.finalUrl'),
    creater,
    gmt_modified,
    date_add(gmt_modified, 15) 
;
