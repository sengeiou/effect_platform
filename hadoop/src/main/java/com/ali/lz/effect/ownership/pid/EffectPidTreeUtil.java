package com.ali.lz.effect.ownership.pid;

import java.util.ArrayList;
import java.util.List;

import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.proto.LzEffectPidProto.PidNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;

public class EffectPidTreeUtil {

    public enum PidItemType {
        NORMAL, TMALL_P4P, TMALL_CPS, TAOBAO_P4P, TAOBAO_CPS;
    }

    public enum PidSrcReferType {
        EMPTY("无"), OUTSIDE("站外"), CHANNEL("频道"), INSIDE("站内其他");

        private PidSrcReferType(String srcReferTypeName) {
            this.srcReferTypeName = srcReferTypeName;
        }

        private String srcReferTypeName;

        public String toString() {
            return this.srcReferTypeName;
        }

    }

    /**
     * 建立树的节点信息
     * 
     * @param node
     */
    public static PTLogEntry genLogEntry(PidNodeValue node) {
        PTLogEntry logEntry = new PTLogEntry();

        // 填充原始日志相关字段信息
        logEntry.put("ts", node.getTs());
        logEntry.put("url", node.getUrl());
        logEntry.put("refer_url", node.getRefer());
        logEntry.put("shop_id", node.getShopId());
        logEntry.put("auction_id", node.getAuctionId());
        logEntry.put("uid", node.getUserId());
        logEntry.put("cookie", node.getCookie());
        logEntry.put("cookie2", node.getCookie2());
        // 建树时会根据logEntry中sid的值来判断是否按session截断
        logEntry.put("sid", "");
        // 填充完整建树结点
        logEntry.put("channel_id", node.getChannelId());
        logEntry.put("refer_channel_id", node.getReferChannelId());
        logEntry.put("is_channel_lp", node.getIsChannelLp());
        logEntry.put("refer_is_channel_lp", node.getReferIsChannelLp());
        logEntry.put("pid", node.getPid());
        logEntry.put("src_refer", node.getSrcRefer());
        logEntry.put("src_refer_type", node.getSrcReferType());
        logEntry.put("refer_src_refer", node.getReferSrcRefer());
        logEntry.put("refer_src_refer_type", node.getReferSrcReferType());
        logEntry.put("pub_id", node.getPubId());
        logEntry.put("site_id", node.getSiteId());
        logEntry.put("adzone_id", node.getAdzoneId());
        logEntry.put("ali_refid", node.getAliRefid());
        logEntry.put("pit_id", node.getPitId());
        logEntry.put("pit_detail", node.getPitDetail());
        logEntry.put("is_effect_page", node.getIsEffectPage());
        logEntry.put("refer_is_effect_page", node.getReferIsEffectPage());
        logEntry.put("item_type", node.getItemType());
        logEntry.put("item_clickid", node.getItemClickid());
        return logEntry;
    }

    /**
     * 
     * @param node
     * @return 返回builder
     */
    public static PidNodeValue.Builder genPidNodeBuilder(HoloTreeNode node) {
        PidNodeValue.Builder builder = PidNodeValue.newBuilder();

        // 用PtLogEntry填充builder
        builder.setTs((Long) node.getPtLogEntry().get("ts"));
        builder.setIndexRootPath((String) node.getSerialRootPath());
        builder.setUrl(((String) node.getPtLogEntry().get("url")).split(Constants.CTRL_A)[0]);
        builder.setRefer(((String) node.getPtLogEntry().get("refer_url")).split(Constants.CTRL_A)[0]);
        builder.setShopId((String) node.getPtLogEntry().get("shop_id"));
        builder.setAuctionId((String) node.getPtLogEntry().get("auction_id"));
        builder.setUserId((String) node.getPtLogEntry().get("uid"));
        builder.setPitId((Integer) node.getPtLogEntry().get("pit_id"));
        builder.setPitDetail((String) node.getPtLogEntry().get("pit_detail"));
        builder.setIsEffectPage((Boolean) node.getPtLogEntry().get("is_effect_page"));
        builder.setReferIsEffectPage((Boolean) node.getPtLogEntry().get("refer_is_effect_page"));
        builder.setPid((String) node.getPtLogEntry().get("pid"));
        builder.setSrcRefer((String) node.getPtLogEntry().get("src_refer"));
        builder.setSrcReferType((String) node.getPtLogEntry().get("src_refer_type"));
        builder.setReferSrcRefer((String) node.getPtLogEntry().get("refer_src_refer"));
        builder.setReferSrcReferType((String) node.getPtLogEntry().get("refer_src_refer_type"));
        builder.setAliRefid((String) node.getPtLogEntry().get("ali_refid"));
        builder.setChannelId((Integer) node.getPtLogEntry().get("channel_id"));
        builder.setReferChannelId((Integer) node.getPtLogEntry().get("refer_channel_id"));
        builder.setCookie((String) node.getPtLogEntry().get("cookie"));
        builder.setCookie2((String) node.getPtLogEntry().get("cookie2"));
        builder.setIsChannelLp((Boolean) node.getPtLogEntry().get("is_channel_lp"));
        builder.setReferIsChannelLp((Boolean) node.getPtLogEntry().get("refer_is_channel_lp"));
        builder.setItemClickid((String) node.getPtLogEntry().get("item_clickid"));
        builder.setItemType((Integer) node.getPtLogEntry().get("item_type"));
        builder.setPubId((String) node.getPtLogEntry().get("pub_id"));
        builder.setSiteId((String) node.getPtLogEntry().get("site_id"));
        builder.setAdzoneId((String) node.getPtLogEntry().get("adzone_id"));
        return builder;
    }

    public static String toString(PidNodeValue node) {

        List<String> tokenA = new ArrayList<String>();
        tokenA.add(String.valueOf(node.getTs()));
        tokenA.add(node.getIndexRootPath());
        tokenA.add(Integer.toString(node.getChannelId()));
        tokenA.add(node.getPid());
        tokenA.add(node.getSrcRefer());
        tokenA.add(node.getSrcReferType());
        tokenA.add(Integer.toString(node.getReferChannelId()));
        tokenA.add(node.getReferSrcRefer());
        tokenA.add(node.getReferSrcReferType());
        tokenA.add(node.getIsChannelLp() ? "1" : "0");
        tokenA.add(node.getReferIsChannelLp() ? "1" : "0");
        tokenA.add(node.getUrl());
        tokenA.add(node.getRefer());
        tokenA.add(node.getShopId());
        tokenA.add(node.getAuctionId());
        tokenA.add(node.getUserId());
        tokenA.add(node.getCookie());
        tokenA.add(node.getCookie2());
        tokenA.add(node.getIsEffectPage() ? "1" : "0");
        tokenA.add(node.getReferIsEffectPage() ? "1" : "0");
        tokenA.add(Integer.toString(node.getPitId()));
        tokenA.add(node.getPitDetail());
        tokenA.add(Integer.toString(node.getItemType()));
        tokenA.add(node.getItemClickid());
        tokenA.add(node.getAliRefid());
        tokenA.add(node.getPubId());
        tokenA.add(node.getSiteId());
        tokenA.add(node.getAdzoneId());
        return StringUtil.join(tokenA, Constants.CTRL_A);
    }

}
