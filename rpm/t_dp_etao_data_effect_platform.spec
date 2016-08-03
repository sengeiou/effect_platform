summary:t_dp_etao_data_effect_platform
Name:t_dp_etao_data_effect_platform
Version:%{_version}
Release:%(echo $RELEASE)%{?dist}
Group: t_dp
License: commerce

BuildArchitectures: noarch
Requires: t_dp_etao_data_effect_platform
BuildRoot: %{_tmppath}/%{name}-%{version}-%{release}

%define _sourcedir ../dist
%define _builddir .
%define _rpmdir .

%description
task for t_dp_etao_data_effect_platform
%prep
%build
%install
dstpath=%{buildroot}/home/taobao/linezing/effect_platform

mkdir -p ${dstpath}

mkdir -p ${dstpath}/config
mkdir -p ${dstpath}/hadoop
mkdir -p ${dstpath}/hadoop/target
mkdir -p ${dstpath}/hadoop/target/lib
mkdir -p ${dstpath}/hadoop/conf
mkdir -p ${dstpath}/load
mkdir -p ${dstpath}/shell
mkdir -p ${dstpath}/hive
mkdir -p ${dstpath}/hive/hive_ddl
mkdir -p ${dstpath}/hive/hive_fact
mkdir -p ${dstpath}/hive/hive_rpt
mkdir -p ${dstpath}/hive/hive_dim

cp ${OLDPWD}/../config/set_env.conf ${dstpath}/config/
cp ${OLDPWD}/../config/client_config.xml ${dstpath}/config/

cp ${OLDPWD}/../hadoop/target/EffectPlatform.jar ${dstpath}/hadoop/target
cp ${OLDPWD}/../hadoop/target/classes/black_list.txt ${dstpath}/hadoop/target
cp ${OLDPWD}/../hadoop/target/classes/white_list.txt ${dstpath}/hadoop/target
#cp ${OLDPWD}/../hadoop/target/lib/protobuf-java-2.4.1.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/commons-collections-3.1.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/commons-logging-1.0.4.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/hbase-0.94-adh3u0.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/hadoop-core-0.20.2-cdh3u3.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/zookeeper-3.4.3.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/guava-r09.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/guava-r09-jarjar.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/log4j-1.2.12.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/slf4j-api-1.6.1.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/slf4j-log4j12-1.6.1.jar ${dstpath}/hadoop/target/lib/
#cp ${OLDPWD}/../hadoop/target/lib/recollection-0.1.jar ${dstpath}/hadoop/target/lib/
cp ${OLDPWD}/../hadoop/conf/log4j.properties ${dstpath}/hadoop/conf/
cp ${OLDPWD}/../hadoop/conf/hbase-site.xml ${dstpath}/hadoop/conf/

cp ${OLDPWD}/../shell/put_plan_conf.py ${dstpath}/shell/
cp ${OLDPWD}/../shell/get_plan_conf.py ${dstpath}/shell/
cp ${OLDPWD}/../shell/del_plan_conf.py ${dstpath}/shell/
cp ${OLDPWD}/../shell/ep_config.py ${dstpath}/shell/
cp ${OLDPWD}/../shell/cp_hadoop_plan_conf.py ${dstpath}/shell/
chmod +x ${dstpath}/shell/*.py

cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_trade_info.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_mid_tree.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_browse_log.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_ad_config.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_ad_click_log.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_ad_click_info.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_ownership_ext.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_cart_info.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_collect_info.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_dim_ep_hjlj_info.sh ${dstpath}/hive/hive_ddl/

cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_trade_info.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_browse_log.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_browse_log_tmall.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_browse_log_lzacookie.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_browse_log_hjlj.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_ownership.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_ownership_ext.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_ad_config.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_ad_click_log.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_ad_click_info.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_cart_info.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_collect_info.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_dim/lz_dim_ep_hjlj_info_tmallbrandsite.sql ${dstpath}/hive/hive_dim/

cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_summary.sh ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_summary_bysrc.sh ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_detail.sh ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_spm_info.sh ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_ad_click_info.sh ${dstpath}/hive/hive_rpt/

cp ${OLDPWD}/../load/lz_rpt_ep_ad_click_info.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_detail.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_summary.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_summary_bysrc.sh ${dstpath}/load/

#### etao effect #####
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_outside_order.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_cps_order.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_tree.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_visit_effect.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_cps_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_outside_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_inside_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_trade_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_channel_visit_effect.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_channel_cps_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_channel_outside_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_channel_inside_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_channel_trade_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_etao_channel_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_summary.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_summary_bysrc.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_summary_bysrc_dataportal.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_tbmarket.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_tblm.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_edm.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_seo.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_seo_limit.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_outside.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_etaosrc.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_taobaosrc.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_tmallsrc.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_pid.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_dataportal.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_bysrc.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_tbmarket.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_tblm.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_edm.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_seo.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_outside.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_etaosrc.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_taobaosrc.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_tmallsrc.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_etao_channel_pid.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_summary.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_summary_bysrc.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_detail.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_ad_click_info.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_spm_info.sh ${dstpath}/hive/hive_ddl/

##### etao effect #######
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_outside_order.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_cps_order.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_tree.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_visit_effect.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_outside_ownership.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_inside_ownership.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_cps_ownership.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_trade_ownership.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_ownership.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_channel_visit_effect.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_channel_cps_ownership.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_channel_outside_ownership.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_channel_inside_ownership.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_channel_trade_ownership.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_etao_channel_ownership.sql ${dstpath}/hive/hive_fact/

cp ${OLDPWD}/../hive/hive_fact/calc_etao_cps_ownership.py ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/calc_etao_outside_ownership.py ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/calc_etao_inside_ownership.py ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/calc_etao_channel_cps_ownership.py ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/calc_etao_channel_outside_ownership.py ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/calc_etao_channel_inside_ownership.py ${dstpath}/hive/hive_fact/

##### etao effect #######
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_summary.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_summary_bysrc.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_summary_bysrc_dataportal.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_tbmarket.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_tblm.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_edm.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_seo.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_seo_limit.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_outside.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_etaosrc.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_taobaosrc.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_tmallsrc.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_pid.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_dataportal.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_bysrc.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_tbmarket.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_tblm.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_edm.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_seo.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_outside.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_etaosrc.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_taobaosrc.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_tmallsrc.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_etao_channel_pid.sql ${dstpath}/hive/hive_rpt/

##### etao effect #######
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_bysrc.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_tbmarket.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_tblm.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_edm.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_seo.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_outside.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_etaosrc.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_taobaosrc.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_tmallsrc.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_etao_channel_pid.sh ${dstpath}/load/


# 月光宝盒V2.3 无线活动效果
cp ${OLDPWD}/../hive/hive_ddl/lz_dim_ep_wireless_yyz.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_dim_ep_wireless_config.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_wireless_log.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_wireless_tree.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_wireless_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_wireless_detail.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_wireless_detail_top.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_wireless_summary.sh ${dstpath}/hive/hive_ddl/

cp ${OLDPWD}/../hive/hive_dim/lz_dim_ep_wireless_yyz.sql ${dstpath}/hive/hive_dim/

cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_wireless_log.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_browse_log_wireless.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_trade_info_wireless.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_wireless_ownership.sh ${dstpath}/hive/hive_fact/

cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_wireless_detail.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_wireless_detail_top.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_wireless_summary.sql ${dstpath}/hive/hive_rpt/

cp ${OLDPWD}/../load/lz_rpt_ep_wireless_detail.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_wireless_summary.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_dim_ep_wireless_yyz.sh ${dstpath}/load/


####月光宝盒联盟效果######
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_pid_tree.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_pid_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_pid_ownership_extend.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_pid_detail.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_pid_summary.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_pid_summary_bysrc.sh ${dstpath}/hive/hive_ddl/

cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_pid_ownership.sh ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_pid_ownership_extend.sql ${dstpath}/hive/hive_fact/

cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_pid_detail.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_pid_summary.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_pid_summary_bysrc.sql ${dstpath}/hive/hive_rpt/

cp ${OLDPWD}/../load/lz_rpt_ep_pid_detail.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_pid_summary.sh ${dstpath}/load/
cp ${OLDPWD}/../load/lz_rpt_ep_pid_summary_bysrc.sh ${dstpath}/load/

####月光宝盒无线客户端效果######
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_client_log.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_fact_ep_client_ownership.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_client_detail.sh ${dstpath}/hive/hive_ddl/
cp ${OLDPWD}/../hive/hive_ddl/lz_rpt_ep_client_summary.sh ${dstpath}/hive/hive_ddl/

cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_client_log.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_browse_log_client.sql ${dstpath}/hive/hive_fact/
cp ${OLDPWD}/../hive/hive_fact/lz_fact_ep_client_ownership.sh ${dstpath}/hive/hive_fact/

cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_client_summary_all.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_client_summary_top.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_client_detail_all.sql ${dstpath}/hive/hive_rpt/
cp ${OLDPWD}/../hive/hive_rpt/lz_rpt_ep_client_detail_top.sql ${dstpath}/hive/hive_rpt/


chmod +x ${dstpath}/hive/hive_ddl/*.sh
chmod +x ${dstpath}/hive/hive_fact/*.sh
chmod +x ${dstpath}/hive/hive_fact/*.py
chmod +x ${dstpath}/hive/hive_rpt/*.sh
chmod +x ${dstpath}/load/*.sh

%files
%defattr(0755,taobao,cug-tbdp)
/home/taobao/linezing/effect_platform

%changelog
* Sat Jun 16 2012 yiyun
- Version 1.0.0
- svn tag address
- http://svn.simba.taobao.com/svn/DW/data_bj/LineZing/lz_data/trunk/effect_platform
