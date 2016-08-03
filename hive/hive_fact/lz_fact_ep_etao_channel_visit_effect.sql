-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:计算频道数据
-- # Input:lz_fact_ep_etao_tree
-- # Onput:lz_fact_ep_etao_channel_visit_effect
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_fact_ep_etao_channel_visit_effect partition (dt='${date}')
select
    is_channel_lp,
    refer_is_channel_lp,
    channel_id,
    channel_src,
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
    cookie,
    user_id,
    sum(1) as pv
from lz_fact_ep_etao_tree
where dt='${date}' and (channel_id > 0 or refer_is_channel_lp=1)
group by
    is_channel_lp,
    refer_is_channel_lp,
    channel_id,
    channel_src,
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
    cookie,
    user_id
