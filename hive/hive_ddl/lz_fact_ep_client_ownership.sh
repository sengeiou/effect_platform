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
drop table lz_fact_ep_client_ownership;

create external table lz_fact_ep_client_ownership
(
   app_key              string comment '应用标识（12278902 代表淘宝android客户端）',
   app_version          string comment '应用版本',
   auction_id           string comment '宝贝ID，非宝贝页为空',
   shop_id              string comment '店铺ID，非店内页面为空',
   user_id              string comment '用户ID，没有为空',
   device_id            string comment '设备ID',
   ip                   string comment 'ip地址（扩展出省份）',
   carrier              string comment '运营商',
   resolution           string comment '屏幕分辨率',
   device_model         string comment '手机型号',
   act_name             string comment '活动名称',
   act_type             bigint comment '活动类型（本地化1  内嵌H5 2）',
   pit_id               bigint comment '坑位id，表示宝贝、店铺和List三类坑位',
   pit_detail           string comment '根据坑位类型，可能为宝贝ID/店铺ID/搜索关键字',
   effect_pv            bigint comment '活动页pv',
   direct_ipv           bigint comment '直接ipv',
   guide_ipv            bigint comment '引导ipv',
   direct_gmv_trade_num bigint comment '直接下单笔数',
   direct_gmv_amt       double comment '直接下单金额',
   direct_alipay_trade_num bigint comment '直接成交笔数',
   direct_alipay_amt    double comment '直接成交金额',
   guide_gmv_trade_num  bigint comment '引导下单笔数',
   guide_gmv_amt        double comment '引导下单金额',
   guide_alipay_trade_num bigint comment '引导成交笔数',
   guide_alipay_amt     double comment '引导成交金额'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_client_ownership';

EOF
