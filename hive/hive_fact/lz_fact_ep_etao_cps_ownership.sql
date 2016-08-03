-- ##########################################################################
-- # Owner:shicheng
-- # Email:shicheng@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:etao网站站外返利的归属
-- # Input:lz_fact_ep_etao_tree, lz_fact_ep_etao_cps_order 
-- # Onput:lz_fact_ep_etao_cps_ownership
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

import file:calc_etao_cps_ownership.py;
import udf:trunc;
insert overwrite table lz_fact_ep_etao_cps_ownership partition (dt='${date}')
select
   transform
   (
            log_type,
            ts              , 
            trade_track_info, 
            user_id         ,     
            lp_src          ,     
            cookie          ,     
            refer_is_lp     ,    
            lp_domain_name  ,
            lp_adid            ,     
            lp_tb_market_id    ,     
            lp_refer_site      ,     
            lp_site_id         ,     
            lp_ad_id           ,
            lp_apply,
            lp_t_id,
            lp_linkname,
            lp_pub_id,
            lp_pid_site_id,
            lp_adzone_id,
            lp_keyword,
            lp_src_domain_name_level1,
            lp_src_domain_name_level2,
            is_new,
            source,
            gmv_num         ,   
            gmv_amt        ,
            joinkey 
   ) row format delimited fields terminated by '\001'
   using 'calc_etao_cps_ownership.py'
   as
   (
            trade_track_info, 
            user_id         ,     
            lp_src          ,     
            cookie          ,     
            refer_is_lp     ,    
            lp_domain_name  ,
            lp_adid            ,     
            lp_tb_market_id    ,     
            lp_refer_site      ,     
            lp_site_id         ,     
            lp_ad_id           ,
            lp_apply,
            lp_t_id,
            lp_linkname,
            lp_pub_id,
            lp_pid_site_id,
            lp_adzone_id,
            lp_keyword,
            lp_src_domain_name_level1,
            lp_src_domain_name_level2,
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
            trade_track_info, 
            user_id         ,     
            lp_src          ,    
            cookie          ,     
            refer_is_lp     ,    
            lp_domain_name  ,
            lp_adid            ,     
            lp_tb_market_id    ,     
            lp_refer_site      ,     
            lp_site_id         ,     
            lp_ad_id           ,
            lp_apply,
            lp_t_id,
            lp_linkname,
            lp_pub_id,
            lp_pid_site_id,
            lp_adzone_id,
            lp_keyword,
            lp_src_domain_name_level1,
            lp_src_domain_name_level2,
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
            trade_track_info, 
            user_id         ,     
            lp_src          ,    
            cookie          ,     
            refer_is_lp     ,    
            lp_domain_name  ,
            lp_adid            ,     
            lp_tb_market_id    ,     
            lp_refer_site      ,     
            lp_site_id         ,     
            lp_ad_id           ,
            lp_apply,
            lp_t_id,
            lp_linkname,
            lp_pub_id,
            lp_pid_site_id,
            lp_adzone_id,
            lp_keyword,
            lp_src_domain_name_level1,
            lp_src_domain_name_level2,
            '' as is_new    ,
            '' as source    ,     
            cast(0 as double) as gmv_num         ,   
            cast(0 as double) as gmv_amt         ,
            case 
                when length(trade_track_info)>1 then trade_track_info 
                else user_id end as joinkey
        from lz_fact_ep_etao_tree
        where dt>=trunc('${date}', 'yyyyMMdd', 'yyyyMMdd', -21) 
            and dt<='${date}' and nodetype='ipv'
            and (length(trade_track_info)>1 or length(user_id)>1 ) 

        union all
        
        select
            '1' as log_type,
            buytime as ts              , 
            cast(trade_track_info as string) as trade_track_info     ,     
            cast(user_id as string) as user_id         ,     
            cast(0 as bigint) as lp_src          ,     
            '' as cookie          ,     
            cast(0 as bigint) as refer_is_lp     ,     
            '' as lp_domain_name  , 
            '' as lp_adid            ,     
            '' as lp_tb_market_id    ,     
            '' as lp_refer_site      ,     
            '' as lp_site_id         ,     
            '' as lp_ad_id           ,     
            '' as lp_apply,
            '' as lp_t_id,
            '' as lp_linkname,
            '' as lp_pub_id,
            '' as lp_pid_site_id,
            '' as lp_adzone_id,
            '' as lp_keyword,
            '' as lp_src_domain_name_level1,
            '' as lp_src_domain_name_level2,
            is_new,
            source,
            gmv_num         ,   
            gmv_amt        ,
            case 
                when source in ('1', '3', '4') then trade_track_info 
                else user_id end as joinkey 
        from lz_fact_ep_etao_cps_order
        where dt='${date}'
    ) a
    distribute by user_id, joinkey
    sort by user_id, joinkey, ts
) b;
