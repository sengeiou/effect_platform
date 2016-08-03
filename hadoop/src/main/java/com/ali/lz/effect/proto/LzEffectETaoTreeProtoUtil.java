package com.ali.lz.effect.proto;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.ownership.etao.ETaoSourceType;
import com.ali.lz.effect.proto.LzEffectETaoTreeProto.ETaoTreeNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;
import com.google.protobuf.InvalidProtocolBufferException;

public class LzEffectETaoTreeProtoUtil {

    public static byte[] serialize(ETaoTreeNodeValue node) {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try {
            node.writeTo(output);
            return output.toByteArray();
        } catch (Exception e) {
        } finally {
            try {
                output.close();
            } catch (Exception e) {
            }
        }
        return null;
    }

    public static ETaoTreeNodeValue deserialize(byte[] data) {
        ETaoTreeNodeValue value = null;
        try {
            value = ETaoTreeNodeValue.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
        }
        return value;
    }

    public static String toString(ETaoTreeNodeValue node) {

        List<String> tokenA = new ArrayList<String>();
        tokenA.add(String.valueOf(node.getTs()));
        tokenA.add(node.getIsEtao() ? "1" : "0");
        tokenA.add(node.getRefIsEtao() ? "1" : "0");

        tokenA.add(node.getIsLp() ? "1" : "0");
        tokenA.add(node.getRefIsLp() ? "1" : "0");
        tokenA.add(String.valueOf(node.getLpSrc()));
        tokenA.add(node.getLpDomainName());
        tokenA.add(node.getLpAdid());
        tokenA.add(node.getLpTbMarketId());
        tokenA.add(node.getLpReferSite());
        tokenA.add(node.getLpSiteId());
        tokenA.add(node.getLpAdId());
        tokenA.add(node.getLpApply());
        tokenA.add(node.getLpTId());
        tokenA.add(node.getLpLinkname());
        tokenA.add(node.getLpPubId());
        tokenA.add(node.getLpPidSiteId());
        tokenA.add(node.getLpAdzoneId());
        tokenA.add(node.getLpKeyword());
        tokenA.add(node.getLpSrcDomainNameLevel1());
        tokenA.add(node.getLpSrcDomainNameLevel2());

        tokenA.add(String.valueOf(node.getChannelId()));
        tokenA.add(String.valueOf(node.getRefChannelId()));
        tokenA.add(node.getIsChannelLp() ? "1" : "0");
        tokenA.add(node.getRefIsChannelLp() ? "1" : "0");
        tokenA.add(String.valueOf(node.getChannelSrc()));
        tokenA.add(String.valueOf(node.getRefChannelSrc()));
        tokenA.add(node.getChannelAdid());
        tokenA.add(node.getChannelTbMarketId());
        tokenA.add(node.getChannelReferSite());
        tokenA.add(node.getChannelSiteId());
        tokenA.add(node.getChannelAdId());
        tokenA.add(node.getChannelApply());
        tokenA.add(node.getChannelTId());
        tokenA.add(node.getChannelLinkname());
        tokenA.add(node.getChannelPubId());
        tokenA.add(node.getChannelPidSiteId());
        tokenA.add(node.getChannelAdzoneId());
        tokenA.add(node.getChannelKeyword());
        tokenA.add(node.getChannelSrcDomainNameLevel1());
        tokenA.add(node.getChannelSrcDomainNameLevel2());

        tokenA.add(node.getRefChannelAdid());
        tokenA.add(node.getRefChannelTbMarketId());
        tokenA.add(node.getRefChannelReferSite());
        tokenA.add(node.getRefChannelSiteId());
        tokenA.add(node.getRefChannelAdId());
        tokenA.add(node.getRefChannelApply());
        tokenA.add(node.getRefChannelTId());
        tokenA.add(node.getRefChannelLinkname());
        tokenA.add(node.getRefChannelPubId());
        tokenA.add(node.getRefChannelPidSiteId());
        tokenA.add(node.getRefChannelAdzoneId());
        tokenA.add(node.getRefChannelKeyword());
        tokenA.add(node.getRefChannelSrcDomainNameLevel1());
        tokenA.add(node.getRefChannelSrcDomainNameLevel2());

        tokenA.add(node.getUrl());
        tokenA.add(node.getRefer());
        tokenA.add(node.getShopId());
        tokenA.add(node.getAuctionId());
        tokenA.add(node.getUserId());
        tokenA.add(node.getCookie());
        tokenA.add(node.getTradeTrackInfo());
        tokenA.add(node.getIndexRootPath());
        tokenA.add(node.getSid());
        tokenA.add(node.getIpvRefUrl());

        return StringUtil.join(tokenA, Constants.CTRL_A);
    }

    public static void setETaoTreeNodeBuilderBySrc(ETaoSourceType channelSourceType, ETaoSourceType lpSourceType,
            ETaoTreeNodeValue.Builder builder) {
        builder.setChannelSrc(channelSourceType.getSrc_id());
        builder.setChannelTbMarketId(channelSourceType.getSourceProperty("tb_market_id"));
        builder.setChannelReferSite(channelSourceType.getSourceProperty("refer_site"));
        builder.setChannelSiteId(channelSourceType.getSourceProperty("site_id"));
        builder.setChannelAdId(channelSourceType.getSourceProperty("ad_id"));
        builder.setChannelApply(channelSourceType.getSourceProperty("apply"));
        builder.setChannelTId(channelSourceType.getSourceProperty("t_id"));
        builder.setChannelLinkname(channelSourceType.getSourceProperty("linkname"));
        builder.setChannelPubId(channelSourceType.getSourceProperty("pub_id"));
        builder.setChannelPidSiteId(channelSourceType.getSourceProperty("pid_site_id"));
        builder.setChannelAdzoneId(channelSourceType.getSourceProperty("adzone_id"));
        builder.setChannelSrcDomainNameLevel1(channelSourceType.getSourceProperty("src_domain_name_level1"));
        builder.setChannelSrcDomainNameLevel2(channelSourceType.getSourceProperty("src_domain_name_level2"));
        builder.setChannelKeyword(channelSourceType.getSourceProperty("keyword"));

        builder.setLpSrc(lpSourceType.getSrc_id());
        builder.setLpTbMarketId(lpSourceType.getSourceProperty("tb_market_id"));
        builder.setLpReferSite(lpSourceType.getSourceProperty("refer_site"));
        builder.setLpSiteId(lpSourceType.getSourceProperty("site_id"));
        builder.setLpAdId(lpSourceType.getSourceProperty("ad_id"));
        builder.setLpApply(lpSourceType.getSourceProperty("apply"));
        builder.setLpTId(lpSourceType.getSourceProperty("t_id"));
        builder.setLpLinkname(lpSourceType.getSourceProperty("linkname"));
        builder.setLpPubId(lpSourceType.getSourceProperty("pub_id"));
        builder.setLpPidSiteId(lpSourceType.getSourceProperty("pid_site_id"));
        builder.setLpAdzoneId(lpSourceType.getSourceProperty("adzone_id"));
        builder.setLpSrcDomainNameLevel1(lpSourceType.getSourceProperty("src_domain_name_level1"));
        builder.setLpSrcDomainNameLevel2(lpSourceType.getSourceProperty("src_domain_name_level2"));
        builder.setLpKeyword(lpSourceType.getSourceProperty("keyword"));
    }

    /**
     * 创建builder, 返回etao node builder
     * 
     * @param node
     * @return 返回builder
     */
    public static ETaoTreeNodeValue.Builder genETaoTreeNodeBuilder(HoloTreeNode node) {
        ETaoTreeNodeValue.Builder builder = ETaoTreeNodeValue.newBuilder();

        // 用PtLogEntry填充builder
        builder.setTs((Long) node.getPtLogEntry().get("ts"));
        builder.setUrl(((String) node.getPtLogEntry().get("url")).split(Constants.CTRL_A)[0]);
        builder.setRefer(((String) node.getPtLogEntry().get("refer_url")).split(Constants.CTRL_A)[0]);
        builder.setShopId((String) node.getPtLogEntry().get("shop_id"));
        builder.setAuctionId((String) node.getPtLogEntry().get("auction_id"));
        builder.setUserId((String) node.getPtLogEntry().get("uid"));
        builder.setCookie((String) node.getPtLogEntry().get("mid_uid"));
        builder.setIsEtao((Boolean) node.getPtLogEntry().get("is_etao"));
        builder.setRefIsEtao((Boolean) node.getPtLogEntry().get("ref_is_etao"));
        builder.setIsLp((Boolean) node.getPtLogEntry().get("is_lp"));
        builder.setRefIsLp((Boolean) node.getPtLogEntry().get("ref_is_lp"));
        builder.setLpDomainName((String) node.getPtLogEntry().get("lp_domain_name"));
        builder.setLpSrc((Integer) node.getPtLogEntry().get("lp_src"));

        builder.setLpTbMarketId((String) node.getPtLogEntry().get("lp_tb_market_id"));
        builder.setLpAdid((String) node.getPtLogEntry().get("lp_adid"));
        builder.setLpReferSite((String) node.getPtLogEntry().get("lp_refer_site"));
        builder.setLpSiteId((String) node.getPtLogEntry().get("lp_site_id"));
        builder.setLpAdId((String) node.getPtLogEntry().get("lp_ad_id"));
        builder.setLpApply((String) node.getPtLogEntry().get("lp_apply"));
        builder.setLpTId((String) node.getPtLogEntry().get("lp_t_id"));
        builder.setLpLinkname((String) node.getPtLogEntry().get("lp_linkname"));
        builder.setLpPubId((String) node.getPtLogEntry().get("lp_pub_id"));
        builder.setLpPidSiteId((String) node.getPtLogEntry().get("lp_pid_site_id"));
        builder.setLpAdzoneId((String) node.getPtLogEntry().get("lp_adzone_id"));
        builder.setLpKeyword((String) node.getPtLogEntry().get("lp_keyword"));
        builder.setLpSrcDomainNameLevel1((String) node.getPtLogEntry().get("lp_src_domain_name_level1"));
        builder.setLpSrcDomainNameLevel2((String) node.getPtLogEntry().get("lp_src_domain_name_level2"));

        builder.setChannelId((Integer) node.getPtLogEntry().get("channel_id"));
        builder.setRefChannelId((Integer) node.getPtLogEntry().get("ref_channel_id"));
        builder.setIsChannelLp((Boolean) node.getPtLogEntry().get("is_channel_lp"));
        builder.setRefIsChannelLp((Boolean) node.getPtLogEntry().get("ref_is_channel_lp"));
        builder.setChannelSrc((Integer) node.getPtLogEntry().get("channel_src"));
        builder.setRefChannelSrc((Integer) node.getPtLogEntry().get("ref_channel_src"));

        builder.setChannelTbMarketId((String) node.getPtLogEntry().get("channel_tb_market_id"));
        builder.setChannelAdid((String) node.getPtLogEntry().get("channel_adid"));
        builder.setChannelReferSite((String) node.getPtLogEntry().get("channel_refer_site"));
        builder.setChannelSiteId((String) node.getPtLogEntry().get("channel_site_id"));
        builder.setChannelAdId((String) node.getPtLogEntry().get("channel_ad_id"));
        builder.setChannelApply((String) node.getPtLogEntry().get("channel_apply"));
        builder.setChannelTId((String) node.getPtLogEntry().get("channel_t_id"));
        builder.setChannelLinkname((String) node.getPtLogEntry().get("channel_linkname"));
        builder.setChannelPubId((String) node.getPtLogEntry().get("channel_pub_id"));
        builder.setChannelPidSiteId((String) node.getPtLogEntry().get("channel_pid_site_id"));
        builder.setChannelAdzoneId((String) node.getPtLogEntry().get("channel_adzone_id"));
        builder.setChannelKeyword((String) node.getPtLogEntry().get("channel_keyword"));
        builder.setChannelSrcDomainNameLevel1((String) node.getPtLogEntry().get("channel_src_domain_name_level1"));
        builder.setChannelSrcDomainNameLevel2((String) node.getPtLogEntry().get("channel_src_domain_name_level2"));

        builder.setRefChannelTbMarketId((String) node.getPtLogEntry().get("ref_channel_tb_market_id"));
        builder.setRefChannelAdid((String) node.getPtLogEntry().get("ref_channel_adid"));
        builder.setRefChannelReferSite((String) node.getPtLogEntry().get("ref_channel_refer_site"));
        builder.setRefChannelSiteId((String) node.getPtLogEntry().get("ref_channel_site_id"));
        builder.setRefChannelAdId((String) node.getPtLogEntry().get("ref_channel_ad_id"));
        builder.setRefChannelApply((String) node.getPtLogEntry().get("ref_channel_apply"));
        builder.setRefChannelTId((String) node.getPtLogEntry().get("ref_channel_t_id"));
        builder.setRefChannelLinkname((String) node.getPtLogEntry().get("ref_channel_linkname"));
        builder.setRefChannelPubId((String) node.getPtLogEntry().get("ref_channel_pub_id"));
        builder.setRefChannelPidSiteId((String) node.getPtLogEntry().get("ref_channel_pid_site_id"));
        builder.setRefChannelAdzoneId((String) node.getPtLogEntry().get("ref_channel_adzone_id"));
        builder.setRefChannelKeyword((String) node.getPtLogEntry().get("ref_channel_keyword"));
        builder.setRefChannelSrcDomainNameLevel1((String) node.getPtLogEntry()
                .get("ref_channel_src_domain_name_level1"));
        builder.setRefChannelSrcDomainNameLevel2((String) node.getPtLogEntry()
                .get("ref_channel_src_domain_name_level2"));

        builder.setTradeTrackInfo((String) node.getPtLogEntry().get("trade_track_info"));
        builder.setIndexRootPath(node.getSerialRootPath());
        builder.setSid((String) node.getPtLogEntry().get("sid"));
        Object ipv_refer_url = node.getPtLogEntry().get("ipv_refer_url");
        if (ipv_refer_url != null) {
            builder.setIpvRefUrl((String) ipv_refer_url);
        }
        return builder;
    }
}
