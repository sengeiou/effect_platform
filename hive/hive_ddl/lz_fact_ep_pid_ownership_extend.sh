#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：南迦
# 邮箱：nanjia.lj@taobao.com
# 日期：20121220
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_fact_ep_pid_ownership_extend;
create external table lz_fact_ep_pid_ownership_extend
(
   channel_id           string comment '活动（频道）id',
   pid                  string comment '来源pid',
   pub_id               string comment 'pub_id',
   site_id              string comment 'site_id',
   site_name            string comment 'site_name',
   adzone_id            string comment 'adzone_id',
   adzone_name          string comment 'adzone_name',
   src_refer            string comment '来源页url',
   src_refer_type       string comment 'src_refer_type',
   url                  string comment 'url',
   refer                string comment 'refer',
   shop_id              string comment '店铺ID，非店内页面为空',
   auction_id           string comment '宝贝ID，非宝贝页为空',
   user_id              string comment '用户ID，没有为空',
   cookie               string comment 'cookie',
   session              string comment 'session',
   visit_id              string comment '计算uv使用',
   is_effect_page       bigint comment '标识活动页',
   refer_is_effect_page bigint comment '标识活动页点击',
   pit_id               bigint comment '坑位id，表示宝贝、店铺和List三类坑位',
   pit_detail           string comment '根据坑位类型，可能为宝贝ID/店铺ID/搜索关键字',
   ali_refid            string comment 'url中解析出的ali_refid（只宝贝坑位计算）',
   effect_pv            bigint comment '活动页一跳pv',
   effect_click_pv      bigint comment '活动页二跳pv',
   channel_pv           bigint comment '活动页总pv',
   guide_ipv            bigint comment '引导ipv',
   direct_gmv_trade_num bigint comment '直接下单笔数',
   direct_gmv_amt       double comment '直接下单金额',
   direct_alipay_trade_num bigint comment '直接成交笔数',
   direct_alipay_amt    double comment '直接成交金额',
   guide_gmv_trade_num  bigint comment '引导下单笔数',
   guide_gmv_amt        double comment '引导下单金额',
   guide_alipay_trade_num bigint comment '引导成交笔数',
   guide_alipay_amt     double comment '引导成交金额',
   p4p_clickid          string comment 'p4p点击id（字符串）',
   p4p_pay_amt          double comment 'p4p点击消耗',
   tbk_clickid          string comment '淘客点击clickid（字符串）',
   tbk_flag             bigint comment '淘客成交标志(默认0 、C店成交1 、B店成交2)',
   tbk_pay_amt_c        double comment '淘客预估c店消耗',
   tbk_pay_amt_b        double comment '淘客预估b店消耗'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_pid_ownership_extend';

EOF
