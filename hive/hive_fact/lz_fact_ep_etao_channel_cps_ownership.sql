-- ##########################################################################
-- # Owner:shicheng
-- # Email:shicheng@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:etao网站站外返利的归属
-- # Input:lz_fact_ep_etao_tree, lz_fact_ep_etao_cps_order 
-- # Onput:lz_fact_ep_etao_channel_cps_ownership
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

import file:calc_etao_channel_cps_ownership.py;
import udf:trunc;
insert overwrite table lz_fact_ep_etao_channel_cps_ownership partition (dt='${date}')
select
   transform
   (
            log_type,
            ts              , 
            refer_channel_id,
            trade_track_info, 
            user_id         ,     
            channel_src     , 
            channel_adid            ,     
            channel_tb_market_id    ,     
            channel_refer_site      ,     
            channel_site_id         ,     
            channel_ad_id           ,
            channel_apply,
            channel_t_id,
            channel_linkname,
            channel_pub_id,
            channel_pid_site_id,
            channel_adzone_id,
            channel_keyword,
            channel_src_domain_name_level1,
            channel_src_domain_name_level2,
            is_new,
            source,
            gmv_num         ,   
            gmv_amt        ,
            joinkey 
   ) row format delimited fields terminated by '\001'
   using 'calc_etao_channel_cps_ownership.py'
   as
   (
            refer_channel_id,
            trade_track_info, 
            user_id         ,     
            channel_src     , 
            channel_adid            ,     
            channel_tb_market_id    ,     
            channel_refer_site      ,     
            channel_site_id         ,     
            channel_ad_id           ,
            channel_apply,
            channel_t_id,
            channel_linkname,
            channel_pub_id,
            channel_pid_site_id,
            channel_adzone_id,
            channel_keyword,
            channel_src_domain_name_level1,
            channel_src_domain_name_level2,
            is_new,
            source,
            gmv_num         ,   
            gmv_amt        
   ) row format delimited fields terminated by '\001'
from
(
    select
            log_type,
            ts              , 
            refer_channel_id,
            trade_track_info, 
            user_id         ,     
            channel_src     , 
            channel_adid            ,     
            channel_tb_market_id    ,     
            channel_refer_site      ,     
            channel_site_id         ,     
            channel_ad_id           ,
            channel_apply,
            channel_t_id,
            channel_linkname,
            channel_pub_id,
            channel_pid_site_id,
            channel_adzone_id,
            channel_keyword,
            channel_src_domain_name_level1,
            channel_src_domain_name_level2,
            is_new,
            source,
            gmv_num         ,   
            gmv_amt        ,
            joinkey 
    from
    (
        select
            '0' as log_type,
            ts              , 
            refer_channel_id,
            trade_track_info, 
            user_id         ,     
            channel_src     , 
            channel_adid            ,     
            channel_tb_market_id    ,     
            channel_refer_site      ,     
            channel_site_id         ,     
            channel_ad_id           ,
            channel_apply,
            channel_t_id,
            channel_linkname,
            channel_pub_id,
            channel_pid_site_id,
            channel_adzone_id,
            channel_keyword,
            channel_src_domain_name_level1,
            channel_src_domain_name_level2,
            '' as is_new    ,
            '' as source    ,     
            cast(0 as double) as gmv_num         ,   
            cast(0 as double) as gmv_amt         ,
            case 
                when length(trade_track_info)>1 then trade_track_info 
                else concat_ws('_', user_id, shop_id) end as joinkey
        from lz_fact_ep_etao_tree
        where dt>=trunc('${date}', 'yyyyMMdd', 'yyyyMMdd', -21) 
            and dt<='${date}' and nodetype='ipv'
            and refer_channel_id > 0
            and (length(trade_track_info)>1 or length(user_id)>1 ) 

        union all
        
        select
            '1' as log_type,
            buytime as ts              , 
            cast(0 as bigint) as refer_channel_id,     
            cast(trade_track_info as string) as trade_track_info     ,     
            cast(user_id as string) as user_id         ,     
            cast(0 as bigint) as channel_src     ,     
            '' as channel_adid            ,     
            '' as channel_tb_market_id    ,     
            '' as channel_refer_site      ,     
            '' as channel_site_id         ,     
            '' as channel_ad_id           ,
            '' as channel_apply,
            '' as channel_t_id,
            '' as channel_linkname,
            '' as channel_pub_id,
            '' as channel_pid_site_id,
            '' as channel_adzone_id,
            '' as channel_keyword,
            '' as channel_src_domain_name_level1,
            '' as channel_src_domain_name_level2,
            is_new,
            source,
            gmv_num         ,   
            gmv_amt        ,
            case 
                when source in ('1', '3', '4') then trade_track_info 
                else concat_ws('_', user_id, shop_id) end as joinkey 
        from lz_fact_ep_etao_cps_order 
        where dt='${date}'
    ) a
    distribute by joinkey
    sort by joinkey, ts
) b;
