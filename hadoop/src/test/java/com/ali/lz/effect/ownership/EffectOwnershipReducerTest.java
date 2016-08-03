package com.ali.lz.effect.ownership;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.Constants.EGroupingKeyType;

public class EffectOwnershipReducerTest {

    public class GenerateNode {
        TreeNodeValue.TypeRef.TypePathInfo.Builder pinfo_builder;
        TreeNodeValue.TypeRef.Builder type_builder;
        TreeNodeValue.Builder builder;

        public GenerateNode() {
            pinfo_builder = TreeNodeValue.TypeRef.TypePathInfo.newBuilder();
            type_builder = TreeNodeValue.TypeRef.newBuilder();
            builder = TreeNodeValue.newBuilder();
        }

        public void setGmvInfo() {
            builder.setGmvAmt(100);
            builder.setGmvAuctionNum(1);
            builder.setGmvTradeNum(2);
            builder.setAlipayAmt(110);
            builder.setAlipayAuctionNum(3);
            builder.setAlipayTradeNum(4);
        }

        public void setLogInfo(int log_type, int ts, String auction_id, String shop_id) {
            builder.setLogType(log_type);
            builder.setTs(ts);
            builder.setShopId(shop_id);
            builder.setAuctionId(auction_id);
            builder.setPageDuration(0);
        }

        public void addPlanInfo(int plan_id) {
            type_builder.setPlanId(plan_id);
            builder.addTypeRef(type_builder);
        }

        public void addPathInfo(String src, int fts, int lts, int priority, boolean is_effect_page,
                boolean ref_is_effect_page, String first_guide_auction_id, String first_guide_shop_id,
                String last_guide_auction_id, String last_guide_shop_id) {
            pinfo_builder.setSrc(src);
            pinfo_builder.setFirstTs(fts);
            pinfo_builder.setLastTs(lts);
            pinfo_builder.setPriority(priority);
            pinfo_builder.setIsEffectPage(is_effect_page);
            pinfo_builder.setRefIsEffectPage(ref_is_effect_page);
            pinfo_builder.setFirstGuideAuctionId(first_guide_auction_id);
            pinfo_builder.setFirstGuideShopId(first_guide_shop_id);
            pinfo_builder.setLastGuideAuctionId(last_guide_auction_id);
            pinfo_builder.setLastGuideShopId(last_guide_shop_id);
            type_builder.addPathInfo(pinfo_builder);
        }

        public byte[] build() {
            return LzEffectProtoUtil.serialize(builder.build());
        }
    }

    ReduceDriver<TextPair, BytesWritable, Text, Text> r;

    @Before
    public void setUp() {
        EffectOwnershipReducer reducer = new EffectOwnershipReducer();
        r = new ReduceDriver<TextPair, BytesWritable, Text, Text>();
        r.setReducer(reducer);
        Configuration conf = new Configuration();
        conf.set(Constants.CONFIG_FILE_PATH, "src/main/resources/config.xml");
        r.setConfiguration(conf);
    }

    /**
     * 测试直接访问
     */
    @Test
    public void testReducer1() {
        TextPair key = new TextPair(new Text(EGroupingKeyType.ACCESS_LOG.getPrefix() + "aa_bb"), new Text("1338697301"));
        List<BytesWritable> values = new ArrayList<BytesWritable>();

        GenerateNode node = new GenerateNode();
        node.setLogInfo(0, 1338697301, "auction_id", "shop_id");
        node.addPathInfo("source1", 10000, 20000, 10, true, true, "a1", "s1", "a2", "s2");
        node.addPathInfo("source2", 10000, 20001, 9, true, true, "a3", "s3", "a3", "s3");
        node.addPlanInfo(1);
        
        byte[] data = node.build();
        values.add(new BytesWritable(data));
        r.withInput(key, values);

        r.withOutput(
                new Text("1"), 
                new Text(
                        "133869730101source2shop_idauction_id0110051.00.00.00.00.00.00.00.00.00.00"));

        r.runTest();
    }

    /**
     * 测试直接访问
     */
    @Test
    public void testReducer2() {
        String input = "134002642100.2truefalsehttp://temai.tmall.com/?spm=3.1000473.197562.2http://www.tmall.com/101499697257484990855710016255755369422129988128101499697426true10310000200003.1000473.197562.2200001340026421134002642110truefalse00-1002147483647truefalse00110.215.102.48Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE---112_201206182133410";
        TreeNodeValue node = LzEffectProtoUtil.fromString(input);

        TextPair key = new TextPair(new Text(EGroupingKeyType.ACCESS_LOG.getPrefix() + "5748499085571001625"),
                new Text("1340026421"));
        List<BytesWritable> values = new ArrayList<BytesWritable>();
        values.add(new BytesWritable(LzEffectProtoUtil.serialize(node)));
        r.withInput(key, values);
        r.withOutput(
                new Text("26"),
                new Text(
                        "0.21340026421426200003.1000473.197562.220000http://temai.tmall.com/?spm=3.1000473.197562.2http://www.tmall.com/101499697257484990855710016255755369422129988128101499697101001.00.00.00.00.00.00.00.00.00.0110.215.102.48Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE---112_201206182133410"));
        r.runTest();

    }

    /**
     * Test Cart ownership.
     */
    @Test
    public void testReducerCart() {
        TextPair key = new TextPair(new Text(EGroupingKeyType.CART_OWNERSHIP.getPrefix() + "5748499085571001625_"),
                new Text("1340026421"));

        String input_access = "134002642100.2truefalsehttp://temai.tmall.com/?spm=3.1000473.197562.2http://www.tmall.com/101499697257484990855710016255755369422129988128101499697426true10310000200003.1000473.197562.2200001340026421134002642110truefalse00-1002147483647truefalse00110.215.102.48Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE---112_201206182133410";
        TreeNodeValue node_access = LzEffectProtoUtil.fromString(input_access);

        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();
        builder.setLogType(3);
        builder.setTs(1340026422);
        builder.setShopId("shop_id");
        builder.setAuctionId("auction_id");
        builder.setUserId("user_id");
        builder.setAliCorp(0);
        builder.setCartNum((float) 1.0);
        TreeNodeValue node_cart = builder.build();

        List<BytesWritable> values = new ArrayList<BytesWritable>();
        values.add(new BytesWritable(LzEffectProtoUtil.serialize(node_access)));
        values.add(new BytesWritable(LzEffectProtoUtil.serialize(node_cart)));
        r.withInput(key, values);

        r.withOutput(
                new Text("26"),
                new Text(
                        "0.21340026421426200003.1000473.197562.220000shop_idauction_iduser_id057484990855710016255755369422129988128101499697000000.00.00.00.00.00.00.00.00.01.0110.215.102.48Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE---112_201206182133410"));
        r.runTest();
    }
}
