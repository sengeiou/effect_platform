package com.ali.lz.effect.extendutils;

import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue.KeyValueS.Builder;

public class GenerateNode {
    public Builder capture_builder;
    public TreeNodeValue.KeyValueI.Builder sourse_builder;
    public TreeNodeValue.TypeRef.TypePathInfo.Builder tpinfo_builder;
    public TreeNodeValue.TypeRef.Builder type_builder;
    public TreeNodeValue.Builder builder;

    public GenerateNode() {
        capture_builder = TreeNodeValue.KeyValueS.newBuilder();
        sourse_builder = TreeNodeValue.KeyValueI.newBuilder();
        tpinfo_builder = TreeNodeValue.TypeRef.TypePathInfo.newBuilder();
        type_builder = TreeNodeValue.TypeRef.newBuilder();
        builder = TreeNodeValue.newBuilder();
    }

    public void setLogInfo(long ts, String url, String refer, String auction_id, String shop_id, String ip, String mid,
            String uid, String sid, String cookie, int ali_corp) {
        builder.setTs(ts);
        builder.setUrl(url);
        builder.setRefer(refer);
        builder.setShopId(shop_id);
        builder.setAuctionId(auction_id);
        builder.setCookie(mid);
        builder.setUserId(uid);
        builder.setSession(sid);
        builder.setCookie(cookie);
        builder.setAliCorp(ali_corp);

    }
    
    public void setPageDuration(long page_duration) {
        builder.setPageDuration(page_duration);
    }

    public void addTreeInfo(boolean is_leaf, boolean is_root, String index_root_path) {
        builder.setIsLeaf(is_leaf);
        builder.setIsRoot(is_root);
        builder.setIndexRootPath(index_root_path);

    }

    public void addinTypeRef(int analyzer_id, int plan_id, boolean is_matched, int PType, int RType) {
        type_builder.setAnalyzerId(analyzer_id);
        type_builder.setPlanId(plan_id);
        type_builder.setIsMatched(is_matched);
        type_builder.setPtype(PType);
        type_builder.setRtype(RType);
    }

    public void addoutTypeRef(int analyzer_id, int plan_id) {
        type_builder.setAnalyzerId(analyzer_id);
        type_builder.setPlanId(plan_id);
    }

    public void addSourceInfo(String key, int value) {
        sourse_builder.setKey(key);
        sourse_builder.setValue(value);
        type_builder.addSourceInfo(sourse_builder);
    }

    public void addCapturedInfo(String key, String value) {
        capture_builder.setKey(key);
        capture_builder.setValue(value);
        type_builder.addCapturedInfo(capture_builder);
    }

    public void addPathInfo(String src, int fts, int lts, int priority) {
        tpinfo_builder.setSrc(src);
        tpinfo_builder.setFirstTs(fts);
        tpinfo_builder.setLastTs(lts);
        tpinfo_builder.setPriority(priority);
        type_builder.addPathInfo(tpinfo_builder);
    }

    public byte[] build() {
        builder.addTypeRef(type_builder);
        return LzEffectProtoUtil.serialize(builder.build());
    }

    public String buildtoString() {
        builder.addTypeRef(type_builder);
        return LzEffectProtoUtil.toString(builder.build());
    }

    public void setGmvInfo(float gmv_amt, float gmv_auction_num, float gmv_trade_num) {
        builder.setGmvAmt(gmv_amt);
        builder.setGmvAuctionNum(gmv_auction_num);
        builder.setGmvTradeNum(gmv_trade_num);
    }

    public void setAlipayInfo(float alipay_amt, float alipay_auction_num, float alipay_trade_num) {
        builder.setAlipayAmt(alipay_amt);
        builder.setAlipayAuctionNum(alipay_auction_num);
        builder.setAlipayTradeNum(alipay_trade_num);
    }

    public void setCollectCartInfo(float shop_collect_num, float item_collect_num, float cart_num) {
        builder.setShopCollectNum(shop_collect_num);
        builder.setItemCollectNum(item_collect_num);
        builder.setCartNum(cart_num);
    }

    public void addAccessUsefulExtra(String key, String value) {
        Builder extra_builder = TreeNodeValue.KeyValueS.newBuilder();
        extra_builder.setKey(key);
        extra_builder.setValue(value);
        builder.addAccessUsefulExtra(extra_builder);
    }

    public void addSrcUsefulExtra(String key, String value) {
        Builder extra_builder = TreeNodeValue.KeyValueS.newBuilder();
        extra_builder.setKey(key);
        extra_builder.setValue(value);
        builder.addSrcUsefulExtra(extra_builder);
    }
}