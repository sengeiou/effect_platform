package com.ali.lz.effect.ownership.wireless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ali.lz.effect.extendutils.MapDriverUtil;
import com.ali.lz.effect.extendutils.MapReduceDriverUtil;
import com.ali.lz.effect.extendutils.ReduceDriverUtil;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.ownership.wireless.EffectWirelessOwnershipMapper;
import com.ali.lz.effect.ownership.wireless.EffectWirelessOwnershipReducer;
import com.ali.lz.effect.proto.LzEffectWirelessProtoUtil;
import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessNodeValue;

public class EffectWirelessOwnershipMRTest {

    private MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text> mapReduceDriver;
    private MapDriverUtil<Object, Text, TextPair, BytesWritable> accessMapDriver;
    private MapDriverUtil<Object, Text, TextPair, BytesWritable> gmvMapDriver;
    private ReduceDriverUtil<TextPair, BytesWritable, Text, Text> reduceDriver;

    EffectWirelessOwnershipMapper.AccessMapper accessMapper = new EffectWirelessOwnershipMapper.AccessMapper();
    EffectWirelessOwnershipMapper.GmvMapper gmvMapper = new EffectWirelessOwnershipMapper.GmvMapper();
    EffectWirelessOwnershipReducer reducer = new EffectWirelessOwnershipReducer();

    @Before
    public void setUp() {

        accessMapDriver = new MapDriverUtil<Object, Text, TextPair, BytesWritable>();
        gmvMapDriver = new MapDriverUtil<Object, Text, TextPair, BytesWritable>();
        reduceDriver = new ReduceDriverUtil<TextPair, BytesWritable, Text, Text>();
        mapReduceDriver = new MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text>();

        accessMapDriver.setMapper(accessMapper);
        gmvMapDriver.setMapper(gmvMapper);
        reduceDriver.setReducer(reducer);

        mapReduceDriver.setMapper(accessMapper);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.setKeyGroupingComparator(new TextPair.RealComparator());

    }

    @Test
    public void testAccessMap() {

        String input = "134002642100.2http://shop.etao.com/redirect.htm/?spm=3.1000473.197562.2&trade_track_info=search_123http://www.tmall.com/11221000012341234";
        accessMapDriver.withInput(new Object(), new Text(input));

        WirelessNodeValue.Builder builder = WirelessNodeValue.newBuilder();

        builder.setLogType(0);
        builder.setTs(1340026421);
        builder.setPlatformId("0");
        builder.setShopId("11");
        builder.setAuctionId("22");
        builder.setUserId("100");
        builder.setCookie("1234");
        builder.setUrl("http://shop.etao.com/redirect.htm/?spm=3.1000473.197562.2&trade_track_info=search_123");
        builder.setRefer("http://www.tmall.com/");
        builder.setIsEffectPage(false);
        builder.setReferIsEffectPage(false);
        builder.setPlanId("1");
        builder.setPitId("2");
        builder.setPitDetail("3");
        builder.setPositionId("4");
        WirelessNodeValue node = builder.build();

        Text user_id_platform_id = new Text(node.getUserId() + "_" + node.getPlatformId());

        accessMapDriver.withOutput(new TextPair(user_id_platform_id, new Text(String.valueOf(node.getTs()))),
                new BytesWritable(LzEffectWirelessProtoUtil.serializeWirelessNodeValue(node)));
        accessMapDriver.runTest();
    }

    @Test
    public void testReduce1() {

        TextPair a = new TextPair(new Text(""), new Text(""));
        List<BytesWritable> c = new ArrayList<BytesWritable>();

        WirelessNodeValue.Builder builder = WirelessNodeValue.newBuilder();

        builder.setLogType(0);
        builder.setTs(1340026421);
        builder.setPlatformId("1");
        builder.setShopId("11");
        builder.setAuctionId("22");
        builder.setUserId("100");
        builder.setCookie("1234");
        builder.setUrl("http://shop.etao.com/redirect.htm/?spm=3.1000473.197562.2&trade_track_info=search_123");
        builder.setRefer("http://www.tmall.com/");
        builder.setIsEffectPage(false);
        builder.setReferIsEffectPage(true);
        builder.setPlanId("123");
        builder.setPitId("1");
        builder.setPitDetail("22");
        builder.setPositionId("");
        WirelessNodeValue node1 = builder.build();

        BytesWritable b1 = new BytesWritable(LzEffectWirelessProtoUtil.serializeWirelessNodeValue(node1));
        c.add(b1);

        WirelessNodeValue.Builder builder2 = WirelessNodeValue.newBuilder();

        builder2.setLogType(1);
        builder2.setTs(1340026422);
        builder2.setPlatformId("1");
        builder2.setShopId("11");
        builder2.setAuctionId("22");
        builder2.setUserId("100");
        builder2.setGmvAuctionNum(1);
        builder2.setGmvTradeAmt(Float.parseFloat("10.10"));
        builder2.setGmvTradeNum(2);
        builder2.setAlipayAuctionNum(3);
        builder2.setAlipayTradeAmt(Float.parseFloat("20.20"));
        builder2.setAlipayTradeNum(4);

        WirelessNodeValue node2 = builder2.build();

        BytesWritable b2 = new BytesWritable(LzEffectWirelessProtoUtil.serializeWirelessNodeValue(node2));
        c.add(b2);

        reduceDriver.withInput(a, c);
        reduceDriver.addOutput(new Text(""), new Text("1221110012340112312201000.000.000.000.0"));
        reduceDriver.addOutput(new Text(""), new Text("12211100123401123122000210.1420.200.000.0"));
        reduceDriver.runTest();

    }

    /**
     * 测试点: 活动规则之间有包含关系时的归属计算(使用扩充的ReduceDriver或MapReduceDriver)
     */
    @Test
    public void testOwnershipReducer2() {
        List<Pair<TextPair, BytesWritable>> mapOutputs = new ArrayList<Pair<TextPair, BytesWritable>>();
        accessMapDriver.setMultiInputsfromFile("src/test/resources/wireless_data/wireless_tree_multirules.output");
        gmvMapDriver.setMultiInputsfromFile("src/test/resources/wireless_data/wireless_ownership_gmv.src");

        try {
            mapOutputs.addAll(accessMapDriver.run());
            mapOutputs.addAll(gmvMapDriver.run());
            TextPair key = null;
            List<BytesWritable> values = new ArrayList<BytesWritable>();
            for (Pair<TextPair, BytesWritable> pair : mapOutputs) {
                key = pair.getFirst();
                if (key.getFirst().toString().endsWith("1")) {
                    values.add(pair.getSecond());
                }
            }
            reduceDriver.runSingleKeyConfTest(key, values, "",
                    "src/test/resources/wireless_data/wireless_ownership_multirules.output");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void testMapReduce1() {

        Map<String, String> m_conf = new HashMap<String, String>();
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/wireless_data/wireless_tree_20121029.src", "",
                "src/test/resources/wireless_data/wireless_tree_20121029.output", m_conf);
    }

}
