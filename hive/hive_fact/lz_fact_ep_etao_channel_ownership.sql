-- ##########################################################################
-- # Owner:luqian
-- # Email:luqian@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:计算频道数据
-- # Input:lz_fact_ep_etao_channel_trade_ownership,lz_fact_ep_etao_channel_visit_effect 
-- # Onput:lz_fact_ep_etao_channel_ownership
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_fact_ep_etao_channel_ownership partition (dt='${date}')
select
     channel_src
    ,channel_id
    ,is_channel_lp
    ,refer_is_channel_lp
    ,refer_channel_src
    ,refer_channel_id
    ,channel_adid                 
    ,channel_tb_market_id         
    ,channel_refer_site           
    ,channel_site_id              
    ,channel_ad_id           
    ,channel_apply
    ,channel_t_id
    ,channel_linkname
    ,channel_pub_id
    ,channel_pid_site_id
    ,channel_adzone_id
    ,channel_keyword
    ,channel_src_domain_name_level1
    ,channel_src_domain_name_level2
    ,ref_channel_adid                 
    ,ref_channel_tb_market_id         
    ,ref_channel_refer_site           
    ,ref_channel_site_id              
    ,ref_channel_ad_id           
    ,ref_channel_apply
    ,ref_channel_t_id
    ,ref_channel_linkname
    ,ref_channel_pub_id
    ,ref_channel_pid_site_id
    ,ref_channel_adzone_id
    ,ref_channel_keyword
    ,ref_channel_src_domain_name_level1
    ,ref_channel_src_domain_name_level2
    ,sum(pv)
    ,cookie
    ,user_id
    ,sum(direct_item_gmv_trade_num             ) as  direct_item_gmv_trade_num         
    ,sum(direct_item_gmv_amt                   ) as  direct_item_gmv_amt               
    ,sum(direct_item_alipay_trade_num          ) as  direct_item_alipay_trade_num      
    ,sum(direct_item_alipay_amt                ) as  direct_item_alipay_amt            
    ,sum(direct_itemshop_gmv_trade_num         ) as  direct_itemshop_gmv_trade_num         
    ,sum(direct_itemshop_gmv_amt               ) as  direct_itemshop_gmv_amt               
    ,sum(direct_itemshop_alipay_trade_num      ) as  direct_itemshop_alipay_trade_num      
    ,sum(direct_itemshop_alipay_amt            ) as  direct_itemshop_alipay_amt            
    ,sum(direct_shop_gmv_trade_num             ) as  direct_shop_gmv_trade_num    
    ,sum(direct_shop_gmv_amt                   ) as  direct_shop_gmv_amt          
    ,sum(direct_shop_alipay_trade_num          ) as  direct_shop_alipay_trade_num 
    ,sum(direct_shop_alipay_amt                ) as  direct_shop_alipay_amt       
    ,sum(outside_alipay_trade_num              ) as  outside_alipay_trade_num     
    ,sum(outside_alipay_amt                    ) as  outside_alipay_amt           
    ,sum(outside_gmv_trade_num                 ) as  outside_gmv_trade_num        
    ,sum(outside_gmv_amt                       ) as  outside_gmv_amt              
    ,sum(cps_new_trade_num                     ) as  cps_new_trade_num            
    ,sum(cps_new_amt                           ) as  cps_new_amt                  
    ,sum(cps_old_trade_num                     ) as  cps_old_trade_num            
    ,sum(cps_old_amt                           ) as  cps_old_amt                  
from
(select 
    channel_src,
    channel_id,
    is_channel_lp,
    refer_is_channel_lp,
    refer_channel_src,
    refer_channel_id,
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
    ref_channel_adid            ,     
    ref_channel_tb_market_id    ,     
    ref_channel_refer_site      ,     
    ref_channel_site_id         ,     
    ref_channel_ad_id           ,
    ref_channel_apply,
    ref_channel_t_id,
    ref_channel_linkname,
    ref_channel_pub_id,
    ref_channel_pid_site_id,
    ref_channel_adzone_id,
    ref_channel_keyword,
    ref_channel_src_domain_name_level1,
    ref_channel_src_domain_name_level2,
    pv,
    cookie,
    user_id,
    0.0 as direct_item_gmv_trade_num,
    0.0 as direct_item_gmv_amt,
    0.0 as direct_item_alipay_trade_num,
    0.0 as direct_item_alipay_amt,            
    0.0 as direct_itemshop_gmv_trade_num,
    0.0 as direct_itemshop_gmv_amt,
    0.0 as direct_itemshop_alipay_trade_num,
    0.0 as direct_itemshop_alipay_amt,            
    0.0 as direct_shop_gmv_trade_num, 
    0.0 as direct_shop_gmv_amt,          
    0.0 as direct_shop_alipay_trade_num, 
    0.0 as direct_shop_alipay_amt, 
    0.0 as outside_alipay_trade_num,  
    0.0 as outside_alipay_amt,           
    0.0 as outside_gmv_trade_num,        
    0.0 as outside_gmv_amt,              
    0.0 as cps_new_trade_num,            
    0.0 as cps_new_amt,                  
    0.0 as cps_old_trade_num,            
    0.0 as cps_old_amt                  
from lz_fact_ep_etao_channel_visit_effect where dt='${date}' and pv > 0

union all

select 
    channel_src,
    refer_channel_id        as channel_id,
    cast(0 as bigint)       as is_channel_lp,
    cast(0 as bigint)       as refer_is_channel_lp,
    cast(0 as bigint)       as refer_channel_src,
    cast(0 as bigint)       as refer_channel_id,
    adid                    as channel_adid,     
    tb_market_id            as channel_tb_market_id,     
    refer_site              as channel_refer_site,     
    site_id                 as channel_site_id,     
    ad_id                   as channel_ad_id,
    apply                   as channel_apply,
    t_id                    as channel_t_id,
    linkname                as channel_linkname,
    pub_id                  as channel_pub_id,
    pid_site_id             as channel_pid_site_id,
    adzone_id               as channel_adzone_id,
    keyword                 as channel_keyword,
    src_domain_name_level1  as channel_src_domain_name_level1,
    src_domain_name_level2  as channel_src_domain_name_level2,
    ''                      as ref_channel_adid,     
    ''                      as ref_channel_tb_market_id,     
    ''                      as ref_channel_refer_site,     
    ''                      as ref_channel_site_id,     
    ''                      as ref_channel_ad_id,
    ''                      as ref_channel_apply,
    ''                      as ref_channel_t_id,
    ''                      as ref_channel_linkname,
    ''                      as ref_channel_pub_id,
    ''                      as ref_channel_pid_site_id,
    ''                      as ref_channel_adzone_id,
    ''                      as ref_channel_keyword,
    ''                      as ref_channel_src_domain_name_level1,
    ''                      as ref_channel_src_domain_name_level2,
    0.0 as pv,
    cast(null as string) as cookie,
    user_id,
    direct_item_gmv_trade_num,
    direct_item_gmv_amt,
    direct_item_alipay_trade_num,
    direct_item_alipay_amt,            
    direct_itemshop_gmv_trade_num,
    direct_itemshop_gmv_amt,
    direct_itemshop_alipay_trade_num,
    direct_itemshop_alipay_amt,            
    direct_shop_gmv_trade_num, 
    direct_shop_gmv_amt,          
    direct_shop_alipay_trade_num, 
    direct_shop_alipay_amt, 
    outside_alipay_trade_num,  
    outside_alipay_amt,           
    outside_gmv_trade_num,        
    outside_gmv_amt,              
    cps_new_trade_num,            
    cps_new_amt,                  
    cps_old_trade_num,            
    cps_old_amt                  
from lz_fact_ep_etao_channel_trade_ownership 
where   
    dt='${date}' 
    and refer_channel_id > 0
) a
group by 
    channel_src,
    channel_id,
    is_channel_lp,
    refer_is_channel_lp,
    refer_channel_src,
    refer_channel_id,
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
    ref_channel_adid            ,     
    ref_channel_tb_market_id    ,     
    ref_channel_refer_site      ,     
    ref_channel_site_id         ,     
    ref_channel_ad_id           ,
    ref_channel_apply,
    ref_channel_t_id,
    ref_channel_linkname,
    ref_channel_pub_id,
    ref_channel_pid_site_id,
    ref_channel_adzone_id,
    ref_channel_keyword,
    ref_channel_src_domain_name_level1,
    ref_channel_src_domain_name_level2,
    user_id,
    cookie
;
