package com.ali.lz.effect.proto;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.ali.lz.effect.proto.LzEffectPidProto.PidNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;
import com.google.protobuf.InvalidProtocolBufferException;

public class LzEffectPidProtoUtil {

    public static byte[] serialize(PidNodeValue node) {
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

    public static PidNodeValue deserialize(byte[] data) {
        PidNodeValue value = null;
        try {
            value = PidNodeValue.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
        }
        return value;
    }

    public static String toString(PidNodeValue node) {

        List<String> tokenA = new ArrayList<String>();
        tokenA.add(Integer.toString(node.getChannelId()));
        tokenA.add(node.getPid());
        tokenA.add(node.getPubId());
        tokenA.add(node.getSiteId());
        tokenA.add(node.getAdzoneId());
        tokenA.add(node.getSrcRefer());
        tokenA.add(node.getSrcReferType());
        tokenA.add(node.getUrl());
        tokenA.add(node.getRefer());
        tokenA.add(node.getShopId());
        tokenA.add(node.getAuctionId());
        tokenA.add(node.getUserId());
        tokenA.add(node.getCookie());
        tokenA.add(node.getSession());
        tokenA.add(node.getCookie2());
        tokenA.add(node.getIsEffectPage() ? "1" : "0");
        tokenA.add(node.getReferIsEffectPage() ? "1" : "0");
        tokenA.add(Integer.toString(node.getPitId()));
        tokenA.add(node.getPitDetail());
        tokenA.add(node.getAliRefid());
        tokenA.add(String.valueOf(node.getEffectPv()));
        tokenA.add(String.valueOf(node.getEffectClickPv()));
        tokenA.add(String.valueOf(node.getChannelPv()));
        tokenA.add(String.valueOf(node.getGuideIpv()));
        tokenA.add(String.valueOf(node.getDirectGmvTradeNum()));
        tokenA.add(String.valueOf(node.getDirectGmvAmt()));
        tokenA.add(String.valueOf(node.getDirectAlipayTradeNum()));
        tokenA.add(String.valueOf(node.getDirectAlipayAmt()));
        tokenA.add(String.valueOf(node.getGuideGmvTradeNum()));
        tokenA.add(String.valueOf(node.getGuideGmvAmt()));
        tokenA.add(String.valueOf(node.getGuideAlipayTradeNum()));
        tokenA.add(String.valueOf(node.getGuideAlipayAmt()));
        tokenA.add(node.getOrderId());
        tokenA.add(node.getP4PClickid());
        tokenA.add(node.getTbkClickid());
        tokenA.add(String.valueOf(node.getTbkFlag()));

        return StringUtil.join(tokenA, Constants.CTRL_A);
    }

    public static PidNodeValue.Builder genBuilder(PidNodeValue value) {

        PidNodeValue.Builder builder = PidNodeValue.newBuilder();

        builder.setLogType(value.getLogType());
        builder.setTs(value.getTs());
        builder.setChannelId(value.getChannelId());
        builder.setAuctionId(value.getAuctionId());
        builder.setShopId(value.getShopId());
        builder.setUserId(value.getUserId());
        builder.setIsEffectPage(value.getIsEffectPage());
        builder.setReferIsEffectPage(value.getReferIsEffectPage());
        builder.setPid(value.getPid());
        builder.setPubId(value.getPubId());
        builder.setSiteId(value.getSiteId());
        builder.setAdzoneId(value.getAdzoneId());
        builder.setSrcRefer(value.getSrcRefer());
        builder.setSrcReferType(value.getSrcReferType());
        // builder.setReferChannelId(value.getReferChannelId());
        // builder.setReferPid(value.getReferPid());
        // builder.setReferSrcRefer(value.getReferSrcRefer());
        // builder.setCookie(value.getCookie());
        // builder.setSession(value.getSession());
        builder.setCookie2(value.getCookie2());
        builder.setPitId(value.getPitId());
        builder.setPitDetail(value.getPitDetail());
        builder.setAliRefid(value.getAliRefid());
        builder.setEffectPv(value.getEffectPv());
        builder.setEffectClickPv(value.getEffectClickPv());
        builder.setChannelPv(value.getChannelPv());
        builder.setGuideIpv(value.getGuideIpv());
        builder.setDirectGmvTradeNum(value.getDirectGmvTradeNum());
        builder.setDirectGmvAmt(value.getDirectGmvAmt());
        builder.setDirectAlipayTradeNum(value.getDirectAlipayTradeNum());
        builder.setDirectAlipayAmt(value.getDirectAlipayAmt());
        builder.setGuideGmvTradeNum(value.getGuideGmvTradeNum());
        builder.setGuideGmvAmt(value.getGuideGmvAmt());
        builder.setGuideAlipayTradeNum(value.getGuideAlipayTradeNum());
        builder.setGuideAlipayAmt(value.getGuideAlipayAmt());
        builder.setP4PClickid(value.getP4PClickid());
        builder.setTbkClickid(value.getTbkClickid());
        builder.setTbkFlag(value.getTbkFlag());
        // builder.setUrl(value.getUrl());
        // builder.setRefer(value.getRefer());
        // builder.setIndexRootPath(value.getIndexRootPath());
        // builder.setGmvTradeNum(value.getGmvTradeNum());
        // builder.setGmvTradeAmt(value.getGmvTradeAmt());
        // builder.setGmvAuctionNum(value.getGmvAuctionNum());
        // builder.setAlipayTradeNum(value.getAlipayTradeNum());
        // builder.setAlipayTradeAmt(value.getAlipayTradeAmt());
        // builder.setAlipayAuctionNum(value.getAlipayAuctionNum());

        return builder;
    }
}
