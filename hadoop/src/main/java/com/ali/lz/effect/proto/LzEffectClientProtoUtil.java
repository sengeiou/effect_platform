package com.ali.lz.effect.proto;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;

import com.ali.lz.effect.proto.LzEffectClientProto.ClientNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;
import com.google.protobuf.InvalidProtocolBufferException;

public class LzEffectClientProtoUtil {

    public static byte[] serializeClientNodeValue(ClientNodeValue node) {
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

    public static ClientNodeValue deserializeClientNodeValue(byte[] data) {
        ClientNodeValue value = null;
        try {
            value = ClientNodeValue.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
        }
        return value;
    }

    public static String toString(ClientNodeValue node) {

        List<String> tokenA = new ArrayList<String>();
        tokenA.add(node.getAppKey());
        tokenA.add(node.getAppVersion());
        tokenA.add(node.getAuctionId());
        tokenA.add(node.getShopId());
        tokenA.add(node.getUserId());
        tokenA.add(node.getDeviceId());
        tokenA.add(node.getIp());
        tokenA.add(node.getCarrier());
        tokenA.add(node.getResolution());
        tokenA.add(node.getDeviceModel());
        tokenA.add(node.getActName());
        tokenA.add(node.getActType());
        tokenA.add(node.getPitId());
        tokenA.add(node.getPitDetail());
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

    public static ClientNodeValue.Builder genBuilder(ClientNodeValue value) {

        ClientNodeValue.Builder builder = ClientNodeValue.newBuilder();

        builder.setLogType(value.getLogType());
        builder.setTs(value.getTs());
        builder.setAppKey(value.getAppKey());
        builder.setAppVersion(value.getAppVersion());
        builder.setEventid(value.getEventid());
        builder.setAuctionId(value.getAuctionId());
        builder.setShopId(value.getShopId());
        builder.setUserId(value.getUserId());
        builder.setDeviceId(value.getDeviceId());
        builder.setIp(value.getIp());
        builder.setCarrier(value.getCarrier());
        builder.setResolution(value.getResolution());
        builder.setDeviceModel(value.getDeviceModel());
        builder.setIsEffectPage(value.getIsEffectPage());
        builder.setReferIsEffectPage(value.getReferIsEffectPage());
        builder.setActName(value.getActName());
        builder.setActType(value.getActType());
        builder.setPitId(value.getPitId());
        builder.setPitDetail(value.getPitDetail());
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
