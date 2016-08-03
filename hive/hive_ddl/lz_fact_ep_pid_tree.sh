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
drop table lz_fact_ep_pid_tree;
create external table lz_fact_ep_pid_tree
(
   ts                   string comment '时间戳',
   index_root_path      string comment '路径列表，用.分割',
   channel_id           string comment '活动（频道）id',
   pid                  string comment '来源pid',
   src_refer            string comment '来源页url',
   src_refer_type       string comment '来源页类型',
   refer_channel_id     string comment 'refer活动（频道）id',
   refer_src_refer      string comment 'refer页来源页url',
   refer_src_refer_type string comment 'refer来源页类型',
   is_channel_lp        bigint comment '标识活动页一跳',
   refer_is_channel_lp  bigint comment '标识活动页二跳',
   url                  string comment 'url',
   refer                string comment 'refer',
   shop_id              string comment '店铺ID，非店内页面为空',
   auction_id           string comment '宝贝ID，非宝贝页为空',
   user_id              string comment '用户ID，没有为空',
   cookie               string comment 'cookie',
   visit_id              string comment '计算uv使用',
   is_effect_page       bigint comment '标识活动页',
   refer_is_effect_page bigint comment '标识活动页点击',
   pit_id               bigint comment '坑位id，表示宝贝、店铺和List三类坑位',
   pit_detail           string comment '根据坑位类型，可能为宝贝ID/店铺ID/搜索关键字',
   item_type            bigint comment '宝贝页类型(0.普通,1.tmallp4p,2.tmalltbk,3.taobaop4p,4.taobaotbk)',
   item_clickid         string comment '宝贝页点击id(ali_trackid解析)',
   ali_refid            string comment 'url参数ali_refid',
   pub_id               string comment 'pub_id, pid格式mm_xxxx_xxxx_xxxx中的第一段数字串',
   site_id              string comment 'site_id, pid格式mm_xxxx_xxxx_xxxx中的第二段数字串',
   adzone_id            string comment 'adzone_id, pid格式mm_xxxx_xxxx_xxxx中的第三段数字串'
)
partitioned by  (dt string)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_pid_tree';

EOF
