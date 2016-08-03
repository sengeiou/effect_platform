#!/bin/bash
source $HOME/linezing/effect_platform/config/set_env.conf

# ------------------------------------------------------------------------
# 作者：feiqiong
# 邮箱：feiqiong.dpf@taobao.com
# 日期：20120611
# ------------------------------------------------------------------------
# 功能：
# 上游：
# ------------------------------------------------------------------------

$Hive <<EOF
drop table lz_fact_ep_mid_tree;
create external table lz_fact_ep_mid_tree 
(
    ts                  string    comment '时间戳',
    log_type            bigint    comment '0 流量日志',
    index_root_path     string    comment '路径列表，用.分割',
    is_leaf             string    comment '是否为叶子节点',
    is_root             string    comment '是否为根节点',
    url                 string    comment 'url',
    refer_url           string    comment 'refer_url',
    shop_id             string    comment '店铺ID，非店内页面为空',
    auction_id          string    comment '宝贝ID，非宝贝页为空',
    user_id             string    comment '用户ID，没有为空',
    ali_corp            bigint    comment '0 未知或非阿里系 1 淘宝 2 天猫 3 一淘 4 聚划算',
    cookie              string    comment 'cookie用来标识一个访问用户（aplus日志中用mid）',
    session             string    comment '一次会话的标识（aplus日志中用sid）',
    visit_id             string    comment '用来计算uv使用（aplus日志中用visit_id, 其他情况直接使用cookie）',
    type_ref            string    comment '染色信息，结构复杂，参见proto',
    useful_extra        string    comment '扩展字段，使用key+ctrlC+value+ctrlB+...方式存储.key为字段名，value为内容。为需要携带，且归属计算中需要使用字段',
    extra               string    comment '扩展字段，使用ctrl+B分割。为需要携带字段',
    src_useful_extra    string    comment '扩展字段，继承自效果页的useful_extra'
)
partitioned by (dt string, planId bigint)
row format delimited fields terminated by '\001' lines terminated by '\n'
stored as sequencefile
location '${hive_fact_dir}/lz_fact_ep_mid_tree'
;
EOF
