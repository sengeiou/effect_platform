-- ##########################################################################
-- # Owner:luqian
-- # Email:luqian@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:汇总成交归属后的结果
-- # Input:lz_fact_ep_etao_channel_cps_ownership, lz_fact_ep_etao_channel_outside_ownership, lz_fact_ep_etao_channel_inside_ownership
-- # Onput:lz_fact_ep_etao_channel_trade_ownership
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################


from
(
  select
    refer_channel_id
    ,channel_src
    ,adid                 
    ,tb_market_id         
    ,refer_site           
    ,site_id              
    ,ad_id           
    ,apply
    ,t_id
    ,linkname
    ,pub_id
    ,pid_site_id
    ,adzone_id
    ,keyword
    ,src_domain_name_level1
    ,src_domain_name_level2
    ,user_id
    ,0.0 as direct_item_gmv_trade_num
    ,0.0 as direct_item_gmv_amt
    ,0.0 as direct_item_alipay_trade_num
    ,0.0 as direct_item_alipay_amt
    ,0.0 as direct_itemshop_gmv_trade_num
    ,0.0 as direct_itemshop_gmv_amt
    ,0.0 as direct_itemshop_alipay_trade_num
    ,0.0 as direct_itemshop_alipay_amt
    ,0.0 as direct_shop_gmv_trade_num
    ,0.0 as direct_shop_gmv_amt
    ,0.0 as direct_shop_alipay_trade_num
    ,0.0 as direct_shop_alipay_amt
    ,case when source in ('1', '3', '4') then cps_trade_num else cast(0 as double) end as outside_gmv_trade_num
    ,case when source in ('1', '3', '4') then cps_amt else cast(0 as double) end as outside_gmv_amt
    ,case when source in ('1', '3', '4') then cps_trade_num else cast(0 as double) end as outside_alipay_trade_num
    ,case when source in ('1', '3', '4') then cps_amt else cast(0 as double) end as outside_alipay_amt
    ,case when is_new='1' then cps_trade_num  else cast(0 as double) end as  cps_new_trade_num
    ,case when is_new='1' then cps_amt        else cast(0 as double) end as  cps_new_amt
    ,case when is_new='0' then cps_trade_num  else cast(0 as double) end as  cps_old_trade_num
    ,case when is_new='0' then cps_amt        else cast(0 as double) end as  cps_old_amt
  from lz_fact_ep_etao_channel_cps_ownership 
    where dt='${date}'
  
  union all

  select
    refer_channel_id
    ,channel_src
    ,adid                 
    ,tb_market_id         
    ,refer_site           
    ,site_id              
    ,ad_id           
    ,apply
    ,t_id
    ,linkname
    ,pub_id
    ,pid_site_id
    ,adzone_id
    ,keyword
    ,src_domain_name_level1
    ,src_domain_name_level2
    ,user_id
    ,0.0 as direct_item_gmv_trade_num
    ,0.0 as direct_item_gmv_amt
    ,0.0 as direct_item_alipay_trade_num
    ,0.0 as direct_item_alipay_amt
    ,0.0 as direct_itemshop_gmv_trade_num
    ,0.0 as direct_itemshop_gmv_amt
    ,0.0 as direct_itemshop_alipay_trade_num
    ,0.0 as direct_itemshop_alipay_amt
    ,0.0 as direct_shop_gmv_trade_num
    ,0.0 as direct_shop_gmv_amt
    ,0.0 as direct_shop_alipay_trade_num
    ,0.0 as direct_shop_alipay_amt
    ,outside_gmv_trade_num
    ,outside_gmv_amt
    ,outside_alipay_trade_num
    ,outside_alipay_amt
    ,0.0 as cps_new_trade_num
    ,0.0 as cps_new_amt
    ,0.0 as cps_old_trade_num
    ,0.0 as cps_old_amt
  from lz_fact_ep_etao_channel_outside_ownership
    where dt='${date}'

  union all

  select
    refer_channel_id
    ,channel_src
    ,adid                 
    ,tb_market_id         
    ,refer_site           
    ,site_id              
    ,ad_id           
    ,apply
    ,t_id
    ,linkname
    ,pub_id
    ,pid_site_id
    ,adzone_id
    ,keyword
    ,src_domain_name_level1
    ,src_domain_name_level2
    ,user_id
    ,direct_item_gmv_trade_num
    ,direct_item_gmv_amt
    ,direct_item_alipay_trade_num
    ,direct_item_alipay_amt
    ,direct_itemshop_gmv_trade_num
    ,direct_itemshop_gmv_amt
    ,direct_itemshop_alipay_trade_num
    ,direct_itemshop_alipay_amt
    ,direct_shop_gmv_trade_num
    ,direct_shop_gmv_amt
    ,direct_shop_alipay_trade_num
    ,direct_shop_alipay_amt
    ,0.0 as outside_gmv_trade_num
    ,0.0 as outside_gmv_amt
    ,0.0 as outside_alipay_trade_num
    ,0.0 as outside_alipay_amt
    ,0.0 as cps_new_trade_num
    ,0.0 as cps_new_amt
    ,0.0 as cps_old_trade_num
    ,0.0 as cps_old_amt
  from lz_fact_ep_etao_channel_inside_ownership
    where dt='${date}'
) a

insert overwrite table lz_fact_ep_etao_channel_trade_ownership partition (dt='${date}')
select
  refer_channel_id
  ,channel_src
  ,adid                 
  ,tb_market_id         
  ,refer_site           
  ,site_id              
  ,ad_id           
  ,apply
  ,t_id
  ,linkname
  ,pub_id
  ,pid_site_id
  ,adzone_id
  ,keyword
  ,src_domain_name_level1
  ,src_domain_name_level2
  ,user_id
  ,sum(direct_item_gmv_trade_num            ) as  direct_item_gmv_trade_num
  ,sum(direct_item_gmv_amt                  ) as  direct_item_gmv_amt
  ,sum(direct_item_alipay_trade_num         ) as  direct_item_alipay_trade_num
  ,sum(direct_item_alipay_amt               ) as  direct_item_alipay_amt
  ,sum(direct_itemshop_gmv_trade_num        ) as  direct_itemshop_gmv_trade_num
  ,sum(direct_itemshop_gmv_amt              ) as  direct_itemshop_gmv_amt
  ,sum(direct_itemshop_alipay_trade_num     ) as  direct_itemshop_alipay_trade_num
  ,sum(direct_itemshop_alipay_amt           ) as  direct_itemshop_alipay_amt
  ,sum(direct_shop_gmv_trade_num            ) as  direct_shop_gmv_trade_num
  ,sum(direct_shop_gmv_amt                  ) as  direct_shop_gmv_amt
  ,sum(direct_shop_alipay_trade_num         ) as  direct_shop_alipay_trade_num
  ,sum(direct_shop_alipay_amt               ) as  direct_shop_alipay_amt
  ,sum(outside_gmv_trade_num                ) as  outside_gmv_trade_num
  ,sum(outside_gmv_amt                      ) as  outside_gmv_amt
  ,sum(outside_alipay_trade_num             ) as  outside_alipay_trade_num
  ,sum(outside_alipay_amt                   ) as  outside_alipay_amt
  ,sum(cps_new_trade_num                    ) as  cps_new_trade_num
  ,sum(cps_new_amt                          ) as  cps_new_amt
  ,sum(cps_old_trade_num                    ) as  cps_old_trade_num
  ,sum(cps_old_amt                          ) as  cps_old_amt
group by
  refer_channel_id
  ,channel_src
  ,adid                 
  ,tb_market_id         
  ,refer_site           
  ,site_id              
  ,ad_id           
  ,apply
  ,t_id
  ,linkname
  ,pub_id
  ,pid_site_id
  ,adzone_id
  ,keyword
  ,src_domain_name_level1
  ,src_domain_name_level2
  ,user_id
  
;
