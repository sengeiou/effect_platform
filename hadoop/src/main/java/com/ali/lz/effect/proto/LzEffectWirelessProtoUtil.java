package com.ali.lz.effect.proto;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessNodeValue;
import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessTreeNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;
import com.google.protobuf.InvalidProtocolBufferException;

public class LzEffectWirelessProtoUtil {

    public static byte[] serializeWirelessNodeValue(WirelessNodeValue node) {
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

    public static WirelessNodeValue deserializeWirelessNodeValue(byte[] data) {
        WirelessNodeValue value = null;
        try {
            value = WirelessNodeValue.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
        }
        return value;
    }

    public static byte[] serializeWirelessTreeNodeValue(WirelessTreeNodeValue node) {
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

    public static WirelessTreeNodeValue deserializeWirelessTreeNodeValue(byte[] data) {
        WirelessTreeNodeValue value = null;
        try {
            value = WirelessTreeNodeValue.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
        }
        return value;
    }

    public static String toString(WirelessNodeValue node) {

        List<String> tokenA = new ArrayList<String>();
        tokenA.add(node.getPlatformId());
        tokenA.add(node.getAuctionId());
        tokenA.add(node.getShopId());
        tokenA.add(node.getUserId());
        tokenA.add(node.getCookie());
        tokenA.add(node.getIsEffectPage() ? "1" : "0");
        tokenA.add(node.getReferIsEffectPage() ? "1" : "0");
        tokenA.add(node.getPlanId());
        tokenA.add(node.getPitId());
        tokenA.add(node.getPitDetail());
        tokenA.add(node.getPositionId());
        tokenA.add(String.valueOf(node.getEffectPv()));
        tokenA.add(String.valueOf(node.getDirectIpv()));
        tokenA.add(String.valueOf(node.getGuideIpv()));
        tokenA.add(String.valueOf(node.getDirectGmvTradeNum()));
        tokenA.add(String.valueOf(node.getDirectGmvAmt()));
        tokenA.add(String.valueOf(node.getDirectAlipayTradeNum()));
        tokenA.add(String.valueOf(node.getDirectAlipayAmt()));
        tokenA.add(String.valueOf(node.getGuideGmvTradeNum()));
        tokenA.add(String.valueOf(node.getGuideGmvAmt()));
        tokenA.add(String.valueOf(node.getGuideAlipayTradeNum()));
        tokenA.add(String.valueOf(node.getGuideAlipayAmt()));

        return StringUtil.join(tokenA, Constants.CTRL_A);
    }

    public static WirelessNodeValue.Builder genBuilder(WirelessNodeValue value) {

        WirelessNodeValue.Builder builder = WirelessNodeValue.newBuilder();

        builder.setLogType(value.getLogType());
        builder.setTs(value.getTs());
        builder.setPlatformId(value.getPlatformId());
        builder.setAuctionId(value.getAuctionId());
        builder.setShopId(value.getShopId());
        builder.setUserId(value.getUserId());
        builder.setCookie(value.getCookie());
        builder.setIsEffectPage(value.getIsEffectPage());
        builder.setReferIsEffectPage(value.getReferIsEffectPage());
        builder.setPlanId(value.getPlanId());
        builder.setPitId(value.getPitId());
        builder.setPitDetail(value.getPitDetail());
        builder.setPositionId(value.getPositionId());
        builder.setEffectPv(value.getEffectPv());
        builder.setDirectIpv(value.getDirectIpv());
        builder.setGuideIpv(value.getGuideIpv());
        builder.setDirectGmvTradeNum(value.getDirectGmvTradeNum());
        builder.setDirectGmvAmt(value.getDirectGmvAmt());
        builder.setDirectAlipayTradeNum(value.getDirectAlipayTradeNum());
        builder.setDirectAlipayAmt(value.getDirectAlipayAmt());
        builder.setGuideGmvTradeNum(value.getGuideGmvTradeNum());
        builder.setGuideGmvAmt(value.getGuideGmvAmt());
        builder.setGuideAlipayTradeNum(value.getGuideAlipayTradeNum());
        builder.setGuideAlipayAmt(value.getGuideAlipayAmt());
        builder.setUrl(value.getUrl());
        builder.setRefer(value.getRefer());
        builder.setGmvTradeNum(value.getGmvTradeNum());
        builder.setGmvTradeAmt(value.getGmvTradeAmt());
        builder.setGmvAuctionNum(value.getGmvAuctionNum());
        builder.setAlipayTradeNum(value.getAlipayTradeNum());
        builder.setAlipayTradeAmt(value.getAlipayTradeAmt());
        builder.setAlipayAuctionNum(value.getAlipayAuctionNum());

        return builder;

    }

}
