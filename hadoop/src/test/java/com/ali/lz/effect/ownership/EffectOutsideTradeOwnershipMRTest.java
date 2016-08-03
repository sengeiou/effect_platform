package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.extendutils.GenerateNode;
import com.ali.lz.effect.extendutils.MapReduceDriverUtil;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.ownership.EffectOutsideTradeOwnershipMapper;
import com.ali.lz.effect.ownership.EffectOutsideTradeOwnershipReducer;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.utils.Constants;

public class EffectOutsideTradeOwnershipMRTest {

    MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text> mr;
    MapDriver<Object, Text, TextPair, BytesWritable> access_map;
    MapDriver<Object, Text, TextPair, BytesWritable> gmv_map;

    ReduceDriver<TextPair, BytesWritable, Text, Text> reduce;

    @Before
    public void setUp() {
        EffectOutsideTradeOwnershipMapper.AccessMapper access_mapper = new EffectOutsideTradeOwnershipMapper.AccessMapper();
        EffectOutsideTradeOwnershipMapper.OutsideTradeMapper gmv_mapper = new EffectOutsideTradeOwnershipMapper.OutsideTradeMapper();
        EffectOutsideTradeOwnershipReducer reducer = new EffectOutsideTradeOwnershipReducer();

        access_map = new MapDriver<Object, Text, TextPair, BytesWritable>();
        access_map.setMapper(access_mapper);
        gmv_map = new MapDriver<Object, Text, TextPair, BytesWritable>();
        gmv_map.setMapper(gmv_mapper);

        reduce = new ReduceDriver<TextPair, BytesWritable, Text, Text>();
        reduce.setReducer(reducer);

        Configuration conf = new Configuration();
        conf.set(Constants.CONFIG_FILE_PATH, "src/test/resources/report_26.xml");
        reduce.setConfiguration(conf);

        mr = new MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text>();
        mr.setMapper(access_mapper);
        mr.setReducer(reducer);
        mr.setKeyGroupingComparator(new TextPair.RealComparator());
    }

    @Test
    public void testAccessMapper() throws IOException {

        String input = "134002642100.2truefalsehttp://shop.etao.com/redirect.htm/?spm=3.1000473.197562.2&trade_track_info=search_123http://www.tmall.com/101499697257484990855710016255755369422129988128101499697426true10310000200003.1000473.197562.2200001340026421134002642110truefalse00-1002147483647truefalse00110.215.102.48Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE---112_201206182133410";
        access_map.withInput(new Object(), new Text(input));

        TreeNodeValue node = LzEffectProtoUtil.fromString(input);
        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder(node);
        TreeNodeValue.KeyValueS.Builder access_useful_extra_builder = TreeNodeValue.KeyValueS.newBuilder();
        access_useful_extra_builder.setKey("trade_track_info");
        access_useful_extra_builder.setValue("search_123");
        builder.addAccessUsefulExtra(access_useful_extra_builder);
        node = builder.build();
        access_map.withOutput(new TextPair(new Text("search_123"), new Text("1340026421")), new BytesWritable(
                LzEffectProtoUtil.serialize(node)));

        access_map.runTest();

    }

    @Test
    public void testOutsideTradeMapper() throws IOException {
        String line = "12345123search_123seller_id123001110.0110.0";
        GenerateNode node1 = new GenerateNode();
        node1.builder.setTs(12345L);
        node1.builder.setUserId("001");
        node1.builder.setAuctionId("123");
        node1.builder.setLogType(4);
        node1.builder.setGmvAmt(10.0f);
        node1.builder.setGmvTradeNum(1f);
        node1.builder.setAlipayAmt(10.0f);
        node1.builder.setAlipayTradeNum(1f);
        node1.addAccessUsefulExtra("trade_track_info", "search_123");
        byte[] data1 = LzEffectProtoUtil.serialize(node1.builder.build());
        gmv_map.withInput(new Text(""), new Text(line));
        gmv_map.withOutput(new TextPair(new Text("search_123"), new Text("12345")), new BytesWritable(data1));
        gmv_map.runTest();
    }

    @Test
    public void testMapReducer() {
        TextPair key = new TextPair(new Text("search_123"), new Text("1340026421"));
        String input = "134002642100.2truefalsehttp://shop.etao.com/redirect.htm/?spm=3.1000473.197562.2&trade_track_info=search_123http://www.tmall.com/101499697257484990855710016255755369422129988128101499697426true10310000200003.1000473.197562.2200001340026421134002642110truefalse00-1002147483647truefalse00110.215.102.48Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE---112_201206182133410";
        TreeNodeValue node = LzEffectProtoUtil.fromString(input);
        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder(node);
        TreeNodeValue.KeyValueS.Builder access_useful_extra_builder = TreeNodeValue.KeyValueS.newBuilder();
        access_useful_extra_builder.setKey("trade_track_info");
        access_useful_extra_builder.setValue("search_123");
        builder.addAccessUsefulExtra(access_useful_extra_builder);
        node = builder.build();

        GenerateNode node1 = new GenerateNode();
        node1.builder.setTs(1340026422L);
        node1.builder.setUserId("001");
        node1.builder.setAuctionId("123");
        node1.builder.setLogType(4);
        node1.builder.setGmvAmt(10.0f);
        node1.builder.setGmvTradeNum(1f);
        node1.builder.setAlipayAmt(10.0f);
        node1.builder.setAlipayTradeNum(1f);
        node1.addAccessUsefulExtra("trade_track_info", "search_123");
        List<BytesWritable> values = new ArrayList<BytesWritable>();
        values.add(new BytesWritable(LzEffectProtoUtil.serialize(node)));
        values.add(new BytesWritable(LzEffectProtoUtil.serialize(node1.builder.build())));
        reduce.withInput(key, values);

        reduce.withOutput(
                new Text("26"),
                new Text(
                        "0.21340026421426200003.1000473.197562.220000123001057484990855710016255755369422129988128101499697000040.010.00.01.010.00.01.00.00.00.0trade_track_infosearch_123110.215.102.48Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.168 Safari/535.19 QIHU 360EE---112_201206182133410"));
        reduce.runTest();
    }
}
