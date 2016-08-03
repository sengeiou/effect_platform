-- ##########################################################################
-- # Owner:shicheng
-- # Email:shicheng@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:etao网站站外的成交归属
-- # Input:lz_fact_ep_etao_tree, lz_fact_ep_etao_outside_order 
-- # Onput:lz_fact_ep_etao_channel_outside_ownership
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

import file:calc_etao_channel_outside_ownership.py;
insert overwrite table lz_fact_ep_etao_channel_outside_ownership partition (dt='${date}')
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
            gmv_trade_num         ,   
            gmv_trade_amt         ,
            pay_trade_num       ,
            pay_trade_amt      
   ) row format delimited fields terminated by '\001'
   using 'calc_etao_channel_outside_ownership.py'
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
            outside_gmv_trade_num         ,   
            outside_gmv_amt         ,
            outside_alipay_trade_num       ,
            outside_alipay_amt      
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
            gmv_trade_num         ,   
            gmv_trade_amt         ,
            pay_trade_num       ,
            pay_trade_amt      
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
            cast(0 as bigint) as gmv_trade_num         ,   
            cast(0 as double) as gmv_trade_amt         ,
            cast(0 as bigint) as pay_trade_num       ,
            cast(0 as double) as pay_trade_amt      
        from lz_fact_ep_etao_tree
        where dt='${date}' 
            and nodetype='ipv' 
            and refer_channel_id > 0
            and length(trade_track_info)>1
        
        union all
        
        select
            '1' as log_type,
            gmv_create_ori_ts as ts              , 
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
            gmv_trade_num         ,   
            gmv_trade_amt         ,
            pay_trade_num       ,
            pay_trade_amt      
        from lz_fact_ep_etao_outside_order 
        where dt='${date}'
    ) a
    distribute by trade_track_info
    sort by trade_track_info, ts
) b;
