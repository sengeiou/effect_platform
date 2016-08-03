-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2013/05/29
-- # ------------------------------------------------------------------------
-- # Description: ETL lz_acookie为月光宝盒基础日志表格式
-- # Input: lz_ods_basic_pvlog
-- # Onput: lz_fact_ep_browse_log
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

import udf:url_decode;
insert overwrite table lz_fact_ep_browse_log partition (dt='${date}', logsrc='lz_acookie')
select 
    time_stamp,
    url,
    ref,
    shop_id,
    case 
        when url_type = 4
        then coalesce(url_index, "")
        else ""
        end as auction_id,
    case 
        when a.user_id <=0 
        then cast(b.user_id as string)
        else cast(a.user_id as string) end,
    a.acookie,
    ss,
    coalesce(cast(a.user_id as string), a.acookie) as visit_id,
    concat_ws('\002',
        concat_ws('\003', 'src_id', src_id),
        concat_ws('\003', 'dp_return', coalesce(dp_return, "0")),
        concat_ws('\003', 'url_return', coalesce(url_return, "")),
        concat_ws('\003', 'url_type', coalesce(url_type, "99")),
        concat_ws('\003', 'ref_type', coalesce(ref_type, "99")),
        concat_ws('\003', 'ref_index', coalesce(ref_index, "")),
        concat_ws('\003', 'key_type', coalesce(key_type, "-99")),
        concat_ws('\003', 'query', coalesce(trim(keyword), "")),
        concat_ws('\003', 'location_id', coalesce(location_id, "")),
        concat_ws('\003', 'src_url', coalesce(reserved3, ""))),
    url_decode(title) as extra
from 
    lz_ods_basic_pvlog a
left outer join
    (select
        cookie,
        max(user_id) as user_id
    from
        lz_fact_ep_browse_log
    where
        dt='${date}'
        and logsrc='aplus'
    group by
        cookie
    )b
on 
    a.dt='${date}' 
    and a.platform='pc'
    and a.log_status='120'
    and a.acookie=b.cookie
;


