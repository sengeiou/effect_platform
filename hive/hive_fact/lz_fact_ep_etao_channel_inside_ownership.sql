-- ##########################################################################
-- # Owner:shicheng
-- # Email:shicheng@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:etao网站的成交归属，属于宽口径。一次计算出来直接和间接
-- # Input:lz_fact_ep_etao_tree, lz_fact_ep_trade_info
-- # Onput:lz_fact_ep_etao_channel_inside_ownership
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

import file:calc_etao_channel_inside_ownership.py;
insert overwrite table lz_fact_ep_etao_channel_inside_ownership partition (dt='${date}')
select
   transform
   (
            log_type,
            ts              , 
            refer_channel_id,     
            shop_id         ,     
            auction_id      ,     
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
            gmv_auction_num       ,
            alipay_trade_num      ,
            alipay_trade_amt      ,
            alipay_auction_num  
   ) row format delimited fields terminated by '\001'
   using 'calc_etao_channel_inside_ownership.py'
   as
   (
            refer_channel_id,     
            shop_id         ,     
            auction_id      ,     
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
            direct_item_gmv_trade_num, 
            direct_item_gmv_amt       ,
            direct_item_alipay_trade_num ,
            direct_item_alipay_amt    ,
            direct_itemshop_gmv_trade_num, 
            direct_itemshop_gmv_amt       ,
            direct_itemshop_alipay_trade_num ,
            direct_itemshop_alipay_amt    ,
            direct_shop_gmv_trade_num ,
            direct_shop_gmv_amt  ,
            direct_shop_alipay_trade_num ,
            direct_shop_alipay_amt 
   ) row format delimited fields terminated by '\001'
from
(
    select
            log_type,
            ts              , 
            refer_channel_id,     
            shop_id         ,     
            auction_id      ,     
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
            gmv_auction_num       ,
            alipay_trade_num      ,
            alipay_trade_amt      ,
            alipay_auction_num  
    from
    (
        select
            '0' as log_type,
            ts              , 
            refer_channel_id,     
            shop_id         ,     
            auction_id      ,     
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
            cast(0 as bigint) as gmv_auction_num       ,
            cast(0 as bigint) as alipay_trade_num      ,
            cast(0 as double) as alipay_trade_amt      ,
            cast(0 as bigint) as alipay_auction_num 
        from lz_fact_ep_etao_tree
        where dt='${date}' 
            and ((nodetype='ipv' and length(auction_id)>0)
                or nodetype='shop')
            and refer_channel_id > 0
            and length(user_id)>0
            and length(shop_id)>0
        
        union all
        
        select
            '1' as log_type,
            gmv_trade_timestamp as ts              , 
            cast(0 as bigint) as refer_channel_id,     
            cast(shop_id as string) as shop_id         ,     
            cast(auction_id as string) as auction_id     ,     
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
