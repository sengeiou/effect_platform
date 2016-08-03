package com.ali.lz.effect.ownership;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.ownership.EffectOwnershipMapper;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.Constants.EGroupingKeyType;
import com.ali.lz.effect.utils.StringUtil;

public class EffectOwnershipMapperTest {
    MapDriver<Object, Text, TextPair, BytesWritable> access_m;
    MapDriver<Object, Text, TextPair, BytesWritable> gmv_m;
    MapDriver<Object, Text, TextPair, BytesWritable> cart_m;

    @Before
    public void setUp() {
        EffectOwnershipMapper.HoloTreeMapper access_mapper = new EffectOwnershipMapper.HoloTreeMapper();
        access_m = new MapDriver<Object, Text, TextPair, BytesWritable>();
        access_m.setMapper(access_mapper);

        EffectOwnershipMapper.GmvMapper gmv_mapper = new EffectOwnershipMapper.GmvMapper();
        gmv_m = new MapDriver<Object, Text, TextPair, BytesWritable>();
        gmv_m.setMapper(gmv_mapper);

        EffectOwnershipMapper.CartMapper cart_mapper = new EffectOwnershipMapper.CartMapper();
        cart_m = new MapDriver<Object, Text, TextPair, BytesWritable>();
        cart_m.setMapper(cart_mapper);
    }

    @Test
    public void testAccessMapper1() {
        TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef.newBuilder();
        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();
        builder.addTypeRef(type_builder);
        TreeNodeValue node = builder.build();

        access_m.withInput(new Object(), new Text(LzEffectProtoUtil.toString(node)));
        access_m.runTest();
    }

    /**
     * Test toString and fromString.
     */
    @Test
    public void testAccessMapper2() {
        TreeNodeValue.KeyValueS.Builder c_builder = TreeNodeValue.KeyValueS.newBuilder();
        TreeNodeValue.KeyValueI.Builder s_builder = TreeNodeValue.KeyValueI.newBuilder();
        TreeNodeValue.TypeRef.TypePathInfo.Builder tpinfo_builder = TreeNodeValue.TypeRef.TypePathInfo.newBuilder();
        TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef.newBuilder();
        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

        type_builder.addCapturedInfo(c_builder);
        type_builder.addSourceInfo(s_builder);
        type_builder.addPathInfo(tpinfo_builder);
        builder.addTypeRef(type_builder);

        TreeNodeValue node = builder.build();

        access_m.withInput(new Object(), new Text(LzEffectProtoUtil.toString(node)));

        TreeNodeValue node2 = LzEffectProtoUtil.fromString(LzEffectProtoUtil.toString(node));
        access_m.withOutput(new TextPair(new Text(EGroupingKeyType.ACCESS_LOG.getPrefix() + node.getCookie()),
                new Text(String.valueOf(node.getTs()))), new BytesWritable(LzEffectProtoUtil.serialize(node2)));

        access_m.runTest();
    }

    @Test
    public void testAccessMapper3() {
        String input = "134002642100.2truefalsehttp://temai.tmall.com/?spm=3.1000473.197562.2http://www.tmall.com/101499697257484990855710016255755369422129988128101499697426true10310000200003.1000473.197562.2200001340026421134002642110truefalse00-1002147483647truefalse00110.215.102.48Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE---112_201206182133410";
        access_m.withInput(new Object(), new Text(input));

        TreeNodeValue node = LzEffectProtoUtil.fromString(input);
        access_m.withOutput(new TextPair(new Text(EGroupingKeyType.ACCESS_LOG.getPrefix() + node.getCookie()),
                new Text("1340026421")), new BytesWritable(LzEffectProtoUtil.serialize(node)));

        access_m.runTest();
    }

    @Test
    public void testGmvMapper() {
        String[] in = { "1338697301", "shop_id", "auction_id", "user_id", "0", "10", "100.00", "20", "9", "90.00",
                "19", "xxxx", "aaaa" };
        String input = StringUtil.join(in, Constants.CTRL_A);

        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();
        builder.setLogType(1); // gmv日志类型为1
        builder.setTs(1338697301);
        builder.setShopId("shop_id");
        builder.setAuctionId("auction_id");
        builder.setUserId("user_id");
        builder.setAliCorp(0);

        builder.setGmvAmt(100);
        builder.setGmvAuctionNum(20);
        builder.setGmvTradeNum(10);
        builder.setAlipayAmt(90);
        builder.setAlipayAuctionNum(19);
        builder.setAlipayTradeNum(9);
        builder.setAccessExtra("aaaa");

        TreeNodeValue node = builder.build();

        gmv_m.withInput(new Object(), new Text(input));
        gmv_m.withOutput(new TextPair(new Text(EGroupingKeyType.GMV_OWNERSHIP.getPrefix() + node.getUserId() + "_"
                + node.getAuctionId()), new Text(String.valueOf(node.getTs()))),
                new BytesWritable(LzEffectProtoUtil.serialize(node)));
        gmv_m.runTest();
    }

    @Test
    public void testCartMapper() {
        String input = "1340026422shop_idauction_iduser_idcookie01useful_extraextra";
        cart_m.withInput(new Object(), new Text(input));

        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();
        builder.setLogType(3);
        builder.setTs(1340026422);
        builder.setShopId("shop_id");
        builder.setAuctionId("auction_id");
        builder.setUserId("user_id");
        builder.setCookie("cookie");
        builder.setAliCorp(0);
        builder.setCartNum((float) 1.0);
        TreeNodeValue node = builder.build();
        cart_m.withOutput(new TextPair(new Text(EGroupingKeyType.CART_OWNERSHIP.getPrefix() + node.getCookie() + "_"
                + node.getAuctionId()), new Text("1340026422")), new BytesWritable(LzEffectProtoUtil.serialize(node)));
        cart_m.runTest();
    }
}
