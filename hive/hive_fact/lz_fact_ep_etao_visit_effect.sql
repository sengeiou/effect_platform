-- ##########################################################################
-- # Owner:feiqiong.dpf
-- # Email:feiqiong.dpf@taobao.com
-- # Date:2012/09/17
-- # ------------------------------------------------------------------------
-- # Description:计算频道数据
-- # Input:lz_fact_ep_etao_tree
-- # Onput:lz_fact_ep_etao_visit_effect
-- # ------------------------------------------------------------------------
-- # ChangeLog:
-- ##########################################################################

insert overwrite table lz_fact_ep_etao_visit_effect partition (dt='${date}')
select
    lp_src,
    lp_domain_name,
    is_lp,
    refer_is_lp,
    is_etao,
    lp_adid,
    lp_tb_market_id,
    lp_refer_site,
    lp_site_id,
    lp_ad_id,
    lp_apply,
    lp_t_id,
    lp_linkname,
    lp_pub_id,
    lp_pid_site_id,
    lp_adzone_id,
    lp_keyword,
    lp_src_domain_name_level1,
    lp_src_domain_name_level2,
    cookie,
    user_id,
    sum(1) as pv
from lz_fact_ep_etao_tree
where dt='${date}' and (is_etao=1 or refer_is_lp=1)
group by
    lp_src,
    lp_domain_name,
    is_lp,
    refer_is_lp,
    is_etao,
    lp_adid,
    lp_tb_market_id,
    lp_refer_site,
    lp_site_id,
    lp_ad_id,
    lp_apply,
    lp_t_id,
    lp_linkname,
    lp_pub_id,
    lp_pid_site_id,
    lp_adzone_id,
    lp_keyword,
    lp_src_domain_name_level1,
    lp_src_domain_name_level2,
    cookie,
    user_id
; 
