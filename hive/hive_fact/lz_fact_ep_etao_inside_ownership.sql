-- ##########################################################################
-- # Owner:shicheng
-- # Email:shicheng@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:etao网站的成交归属，属于宽口径。一次计算出来直接和间接
-- # Input:lz_fact_ep_etao_tree, lz_fact_ep_trade_info
-- # Onput:lz_fact_ep_etao_inside_ownership
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

import file:calc_etao_inside_ownership.py;
insert overwrite table lz_fact_ep_etao_inside_ownership partition (dt='${date}')
select
   transform
   (
            log_type,
            ts              , 
            shop_id         ,     
            auction_id      ,     
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
            gmv_trade_num         ,   
            gmv_trade_amt         ,
            gmv_auction_num       ,
            alipay_trade_num      ,
            alipay_trade_amt      ,
            alipay_auction_num  
   ) row format delimited fields terminated by '\001'
   using 'calc_etao_inside_ownership.py'
   as
   (
            shop_id         ,     
            auction_id      ,     
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
            direct_item_gmv_trade_num, 
            direct_item_gmv_amt       ,
            direct_item_alipay_trade_num ,
            direct_item_alipay_amt    ,
            direct_itemshop_gmv_trade_num ,
            direct_itemshop_gmv_amt  ,
            direct_itemshop_alipay_trade_num ,
            direct_itemshop_alipay_amt 
   ) row format delimited fields terminated by '\001'
from
(
    select
            log_type,
            ts              , 
            shop_id         ,     
            auction_id      ,     
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
            gmv_trade_num         ,   
            gmv_trade_amt         ,
            gmv_auction_num       ,
            alipay_trade_num      ,
            alipay_trade_amt      ,
            alipay_auction_num  
    from
    (
        select
            '0' as log_type,
            ts              , 
            shop_id         ,     
            auction_id      ,     
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
            cast(0 as bigint) as gmv_trade_num         ,   
            cast(0 as double) as gmv_trade_amt         ,
            cast(0 as bigint) as gmv_auction_num       ,
            cast(0 as bigint) as alipay_trade_num      ,
            cast(0 as double) as alipay_trade_amt      ,
            cast(0 as bigint) as alipay_auction_num  
        from lz_fact_ep_etao_tree
        where dt='${date}' 
            and nodetype='ipv' 
            and length(auction_id)>0 
            and length(user_id)>0
            and length(shop_id)>0
        
        union all
        
        select
            '1' as log_type,
            gmv_trade_timestamp as ts              , 
            cast(shop_id as string) as shop_id         ,     
            cast(auction_id as string) as auction_id     ,     
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
            gmv_trade_num         ,   
            gmv_trade_amt         ,
            gmv_auction_num       ,
            alipay_trade_num      ,
            alipay_trade_amt      ,
            alipay_auction_num  
        from lz_fact_ep_trade_info
        where dt='${date}' and round=11 and logsrc='taobao'
    ) a
    distribute by user_id, shop_id
    sort by user_id, shop_id, ts
) b;
