#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：南迦
# 邮箱：nanjia.lj@taobao.com
# 日期：20130123
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_rpt_ep_client_summary;

create external table lz_rpt_ep_client_summary
(
   day                  string comment '日期',
   province             string comment '省份',
   carrier              string comment '运营商',
   resolution           string comment '屏幕分辨率',
   device_model         string comment '手机型号',
   act_name             string comment '活动名称',
   act_type             bigint comment '活动类型（本地化1  内嵌H5 2）',
   effect_pv            bigint comment '活动页总pv',
   effect_uv            bigint comment '活动页总uv',
   direct_ipv           bigint comment '直接ipv',
   direct_iuv           bigint comment '直接iuv',
   direct_gmv_uv        bigint comment '直接下单uv',
   direct_gmv_trade_num bigint comment '直接下单笔数',
   direct_gmv_amt       double comment '直接下单金额',
   direct_alipay_uv     bigint comment '直接成交uv',
   direct_alipay_trade_num bigint comment '直接成交笔数',
   direct_alipay_amt    double comment '直接成交金额',
   guide_ipv            bigint comment '引导ipv',
   guide_iuv            bigint comment '引导iuv',
   guide_gmv_uv         bigint comment '引导下单uv',
   guide_gmv_trade_num  bigint comment '引导下单笔数',
   guide_gmv_amt        double comment '引导下单金额',
   guide_alipay_uv      bigint comment '引导成交uv',
   guide_alipay_trade_num bigint comment '引导成交笔数',
   guide_alipay_amt     double comment '引导成交金额'
)
partitioned by  (dt string,type string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_rpt_dir}/lz_rpt_ep_client_summary';

EOF
