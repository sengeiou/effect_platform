-- ##########################################################################
-- # Owner:luqian
-- # Email:luqian@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:ETL cps表
-- # Input:s_dw_etao_cps_order
-- # Onput:lz_fact_ep_etao_cps_order
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_fact_ep_etao_cps_order partition (dt='${date}')
select
    source
    ,trade_no
    ,trade_track_info
    ,case when a.source='2' then cast(b.shop_id as string) else a.shop_id end
    ,is_settle
    ,a.seller_id
    ,userid as user_id
    ,gmv_num
    ,gmv_amt
    ,unix_timestamp(buytime,'yyyy-MM-dd HH:mm:ss' ) as buytime
    ,unix_timestamp(checktime,'yyyy-MM-dd HH:mm:ss' ) as checktime
    ,is_new
from 
    s_dw_etao_cps_order a 
    left outer join lz_dim_sellers b
on 
    a.ds='${date}' 
    and b.dt='${date}' 
    and a.seller_id=b.seller_id
where 
    a.is_settle=1 
    and a.gmv_num>0
;
