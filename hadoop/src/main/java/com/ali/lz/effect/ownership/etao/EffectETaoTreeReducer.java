package com.ali.lz.effect.ownership.etao;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.holotree.HoloTreeBuilder;
import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.proto.LzEffectETaoTreeProtoUtil;
import com.ali.lz.effect.proto.LzEffectETaoTreeProto.ETaoTreeNodeValue;

public class EffectETaoTreeReducer extends MapReduceBase implements Reducer<TextPair, BytesWritable, Text, Text> {

    /**
     * 建立树的节点信息
     * 
     * @param node
     */
    private PTLogEntry genLogEntry(ETaoTreeNodeValue node) {
        PTLogEntry logEntry = new PTLogEntry();
        List<String> token = new ArrayList<String>();

        // 填充原始日志相关字段信息
        logEntry.put("ts", node.getTs());
        logEntry.put("url", node.getUrl());
        logEntry.put("refer_url", node.getRefer());
        logEntry.put("shop_id", node.getShopId());
        logEntry.put("auction_id", node.getAuctionId());
        logEntry.put("uid", node.getUserId());
        logEntry.put("mid", node.getCookie());
        logEntry.put("mid_uid", node.getCookie());
        // 建树时会根据logEntry中sid的值来判断是否按session截断
        logEntry.put("sid", node.getSid());
        // 填充完整建树结点
        logEntry.put("is_etao", node.getIsEtao());
        logEntry.put("ref_is_etao", node.getRefIsEtao());
        logEntry.put("is_lp", node.getIsLp());
        logEntry.put("ref_is_lp", node.getRefIsLp());
        logEntry.put("lp_src", node.getLpSrc());
        logEntry.put("lp_domain_name", node.getLpDomainName());
        logEntry.put("lp_adid", node.getLpAdid());
        logEntry.put("lp_tb_market_id", node.getLpTbMarketId());
        logEntry.put("lp_refer_site", node.getLpReferSite());
        logEntry.put("lp_site_id", node.getLpSiteId());
        logEntry.put("lp_ad_id", node.getLpAdId());
        logEntry.put("lp_apply", node.getLpApply());
        logEntry.put("lp_t_id", node.getLpTId());
        logEntry.put("lp_linkname", node.getLpLinkname());
        logEntry.put("lp_pub_id", node.getLpPubId());
        logEntry.put("lp_pid_site_id", node.getLpPidSiteId());
        logEntry.put("lp_adzone_id", node.getLpAdzoneId());
        logEntry.put("lp_src_domain_name_level1", node.getLpSrcDomainNameLevel1());
        logEntry.put("lp_src_domain_name_level2", node.getLpSrcDomainNameLevel2());
        logEntry.put("lp_keyword", node.getLpKeyword());

        logEntry.put("channel_id", node.getChannelId());
        logEntry.put("ref_channel_id", node.getRefChannelId());
        logEntry.put("is_channel_lp", node.getIsChannelLp());
        logEntry.put("ref_is_channel_lp", node.getRefIsChannelLp());
        logEntry.put("channel_src", node.getChannelSrc());
        logEntry.put("ref_channel_src", node.getRefChannelSrc());

        logEntry.put("channel_adid", node.getChannelAdid());
        logEntry.put("channel_tb_market_id", node.getChannelTbMarketId());
        logEntry.put("channel_refer_site", node.getChannelReferSite());
        logEntry.put("channel_site_id", node.getChannelSiteId());
        logEntry.put("channel_ad_id", node.getChannelAdId());
        logEntry.put("channel_apply", node.getChannelApply());
        logEntry.put("channel_t_id", node.getChannelTId());
        logEntry.put("channel_linkname", node.getChannelLinkname());
        logEntry.put("channel_pub_id", node.getChannelPubId());
        logEntry.put("channel_pid_site_id", node.getChannelPidSiteId());
        logEntry.put("channel_adzone_id", node.getChannelAdzoneId());
        logEntry.put("channel_src_domain_name_level1", node.getChannelSrcDomainNameLevel1());
        logEntry.put("channel_src_domain_name_level2", node.getChannelSrcDomainNameLevel2());
        logEntry.put("channel_keyword", node.getChannelKeyword());

        logEntry.put("ref_channel_adid", node.getRefChannelAdid());
        logEntry.put("ref_channel_tb_market_id", node.getRefChannelTbMarketId());
        logEntry.put("ref_channel_refer_site", node.getRefChannelReferSite());
        logEntry.put("ref_channel_site_id", node.getRefChannelSiteId());
        logEntry.put("ref_channel_ad_id", node.getRefChannelAdId());
        logEntry.put("ref_channel_apply", node.getRefChannelApply());
        logEntry.put("ref_channel_t_id", node.getRefChannelTId());
        logEntry.put("ref_channel_linkname", node.getRefChannelLinkname());
        logEntry.put("ref_channel_pub_id", node.getRefChannelPubId());
        logEntry.put("ref_channel_pid_site_id", node.getRefChannelPidSiteId());
        logEntry.put("ref_channel_adzone_id", node.getRefChannelAdzoneId());
        logEntry.put("ref_channel_src_domain_name_level1", node.getRefChannelSrcDomainNameLevel1());
        logEntry.put("ref_channel_src_domain_name_level2", node.getRefChannelSrcDomainNameLevel2());
        logEntry.put("ref_channel_keyword", node.getRefChannelKeyword());
        logEntry.put("trade_track_info", node.getTradeTrackInfo());

        return logEntry;
    }

    public void reduce(TextPair key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {

        // 建树过程
        HoloTreeBuilder builder = new HoloTreeBuilder(new HoloConfig());
        builder.setDoPathMatch(false);
        while (values.hasNext()) {
            BytesWritable nodeData = values.next();
            ETaoTreeNodeValue nodeValue = LzEffectETaoTreeProtoUtil.deserialize(Arrays.copyOf(nodeData.getBytes(),
                    nodeData.getLength()));
            if (nodeValue == null) {
                continue;
            }

            PTLogEntry logEntry = genLogEntry(nodeValue);
            builder.appendLog(logEntry);
        }

        // 循环遍历树并输出每棵树的结点
        for (SortedMap<Long, HoloTreeNode> holoTree : builder.getCurrentTrees()) {
            Iterator<HoloTreeNode> it = holoTree.values().iterator();
            while (it.hasNext()) {
                // 轮询树中的每个树结点
                HoloTreeNode node = it.next();
                ETaoTreeNodeValue nodeValue = EffectETaoTreeUtil.genETaoTreeNodeValue(node);
                if (nodeValue.getTradeTrackInfo().length() > 0 || nodeValue.getAuctionId().length() > 0) {
                    output.collect(new Text("ipv"), new Text(LzEffectETaoTreeProtoUtil.toString(nodeValue)));
                } else if (nodeValue.getShopId().length() > 0) {
                    output.collect(new Text("shop"), new Text(LzEffectETaoTreeProtoUtil.toString(nodeValue)));
                } else {
                    output.collect(new Text("others"), new Text(LzEffectETaoTreeProtoUtil.toString(nodeValue)));
                }
            }
        }

        // 清空建树器，初始化计数器，开始另一轮建树
        builder.flush();
    }

}
