package com.ali.lz.effect.ownership.wireless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessTreeNodeValue;
import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessTreeNodeValue.PlanProperty;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;

public class EffectWirelessTreeUtil {

    /**
     * 建立树的节点信息
     * 
     * @param node
     */
    public static PTLogEntry genLogEntry(WirelessTreeNodeValue node) {
        PTLogEntry logEntry = new PTLogEntry();
        List<String> token = new ArrayList<String>();

        // 填充原始日志相关字段信息
        logEntry.put("ts", node.getTs());
        logEntry.put("url", node.getUrl());
        logEntry.put("refer_url", node.getRefer());
        logEntry.put("shop_id", node.getShopId());
        logEntry.put("auction_id", node.getAuctionId());
        logEntry.put("uid", node.getUserId());
        logEntry.put("cookie", node.getCookie());
        // 建树时会根据logEntry中sid的值来判断是否按session截断
        logEntry.put("sid", "");
        // 填充完整建树结点
        logEntry.put("platform_id", node.getPlatformId());
        logEntry.put("plan_properties", new ArrayList<PlanProperty>(node.getPlanPropertiesList()));
        logEntry.put("position_id", node.getPositionId());
        return logEntry;
    }

    /**
     * 创建builder, 返回wireless node builder
     * 
     * @param node
     * @return 返回builder
     */
    public static WirelessTreeNodeValue.Builder genWirelessTreeNodeBuilder(HoloTreeNode node) {
        WirelessTreeNodeValue.Builder builder = WirelessTreeNodeValue.newBuilder();

        // 用PtLogEntry填充builder
        builder.setTs((Long) node.getPtLogEntry().get("ts"));
        builder.setIndexRootPath((String) node.getSerialRootPath());
        builder.setUrl(((String) node.getPtLogEntry().get("url")).split(Constants.CTRL_A)[0]);
        builder.setRefer(((String) node.getPtLogEntry().get("refer_url")).split(Constants.CTRL_A)[0]);
        builder.setShopId((String) node.getPtLogEntry().get("shop_id"));
        builder.setAuctionId((String) node.getPtLogEntry().get("auction_id"));
        builder.setUserId((String) node.getPtLogEntry().get("uid"));
        builder.setPlatformId((String) node.getPtLogEntry().get("platform_id"));
        builder.addAllPlanProperties((List<PlanProperty>) node.getPtLogEntry().get("plan_properties"));
        builder.setPositionId((String) node.getPtLogEntry().get("position_id"));
        builder.setCookie((String) node.getPtLogEntry().get("cookie"));
        return builder;
    }

    public static void output(WirelessTreeNodeValue node, OutputCollector<Text, Text> output) throws IOException {
        if (node.getPlanPropertiesCount() > 0) {
            for (PlanProperty property : node.getPlanPropertiesList()) {
                output.collect(new Text(""), new Text(toString(node, property)));
            }
        } else {
            PlanProperty property = PlanProperty.newBuilder().build();
            output.collect(new Text(""), new Text(toString(node, property)));
        }
    }

    // public static String toString(WirelessTreeNodeValue node) {
    //
    // StringBuilder sb = new StringBuilder();
    // if (node.getPlanPropertiesCount() > 0) {
    // for (PlanProperty property : node.getPlanPropertiesList()) {
    // sb.append(joinWirelessTreeNodeValue(node, property));
    // sb.append(Constants.NEWLINE);
    // }
    // sb.setLength(sb.length() - Constants.NEWLINE.length());
    // return sb.toString();
    // } else {
    // PlanProperty property = PlanProperty.newBuilder().build();
    // return joinWirelessTreeNodeValue(node, property);
    // }
    // }

    private static String toString(WirelessTreeNodeValue node, PlanProperty property) {
        List<String> tokenA = new ArrayList<String>();
        tokenA.add(String.valueOf(node.getTs()));
        tokenA.add(node.getPlatformId());
        tokenA.add(node.getIndexRootPath());
        tokenA.add(node.getUrl());
        tokenA.add(node.getRefer());
        tokenA.add(node.getShopId());
        tokenA.add(node.getAuctionId());
        tokenA.add(node.getUserId());
        tokenA.add(property.getIsEffectPage() ? "1" : "0");
        tokenA.add(property.getReferIsEffectPage() ? "1" : "0");
        tokenA.add(property.getPlanId());
        tokenA.add(property.getPitId());
        tokenA.add(property.getPitDetail());
        tokenA.add(node.getPositionId());
        tokenA.add(node.getCookie());
        return StringUtil.join(tokenA, Constants.CTRL_A);
    }

}
