-- ##########################################################################
-- # Owner:nanjia
-- # Email:nanjia.lj@taobao.com
-- # Date:2012/11/22
-- # ------------------------------------------------------------------------
-- # Description:为聚石塔准备收藏数据ETL
-- # Input:r_collect_item_info_d
-- # Onput:lz_fact_ep_collect_info
-- ##########################################################################

insert overwrite table lz_fact_ep_collect_info partition (dt='${date}', logsrc='taobao')
select 
    cast(unix_timestamp(collect_time,'yyyy-MM-dd HH:mm:ss') as bigint) as collect_timestamp,
    collect_type,
    shop_id,
    auction_id,
    buyer_id as user_id,
    cast(0 as bigint) as ali_corp,
    cast(1 as bigint) as collect_num,
    '' as useful_extra,
    '' as extra
from 
    sdm_xxdf_collect_info
where
    dt = '${date}'
;
