#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：南迦
# 邮箱：nanjia.lj@taobao.com
# 日期：20121213
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_rpt_ep_pid_detail;
create external table lz_rpt_ep_pid_detail
(
   day                  string comment '日期',
   channel_id           string comment '活动（频道）id',
   pit_id               bigint comment '坑位id，表示宝贝、店铺和List三类坑位',
   pit_detail           string comment '根据坑位类型，可能为宝贝ID/店铺ID/搜索关键字',
   ali_refid            string comment 'ali_refid（只宝贝坑位计算）',
   effect_click_pv      bigint comment '效果页二跳pv',
   effect_click_uv      bigint comment '效果页二跳uv',
   guide_ipv            bigint comment '引导ipv',
   guide_iuv            bigint comment '引导iuv',
   direct_gmv_uv        bigint comment '直接下单uv',
   direct_gmv_trade_num bigint comment '直接下单笔数',
   direct_gmv_amt       double comment '直接下单金额',
   direct_alipay_uv     bigint comment '直接成交uv',
   direct_alipay_trade_num bigint comment '直接成交笔数',
   direct_alipay_amt    double comment '直接成交金额',
   guide_gmv_uv         bigint comment '引导下单uv',
   guide_gmv_trade_num  bigint comment '引导下单笔数',
   guide_gmv_amt        double comment '引导下单金额',
   guide_alipay_uv      bigint comment '引导成交uv',
   guide_alipay_trade_num bigint comment '引导成交笔数',
   guide_alipay_amt     double comment '引导成交金额',
   p4p_click_num        bigint comment 'p4p有效点击',
   p4p_pay_amt          double comment 'p4p有效消耗',
   tbk_click_num        bigint comment '淘客点击数',
   tbk_pay_amt_c        double comment '预估C店成交佣金',
   tbk_pay_amt_b        double comment '预估B店成交佣金'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_rpt_dir}/lz_rpt_ep_pid_detail';

EOF
