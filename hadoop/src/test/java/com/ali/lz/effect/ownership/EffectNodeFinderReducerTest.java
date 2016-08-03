package com.ali.lz.effect.ownership;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.extendutils.GenerateNode;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.ownership.EffectNodeFinderReducer;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.utils.Constants;

public class EffectNodeFinderReducerTest {

    private EffectNodeFinderReducer reducer;
    private ReduceDriver<TextPair, BytesWritable, Text, Text> reduceDriver;

    @Before
    public void setUp() throws Exception {
        reducer = new EffectNodeFinderReducer();
        reduceDriver = new ReduceDriver<TextPair, BytesWritable, Text, Text>(reducer);
        Configuration conf = new Configuration();
        conf.set(Constants.CONFIG_FILE_PATH, "src/main/resources/config.xml");
        reduceDriver.setConfiguration(conf);
    }

    @Test
    public void testReduce1() {
        Text mid_sid = new Text("aa_bb");
        Text ts = new Text("1338697301");
        TextPair key = new TextPair(mid_sid, ts);

        GenerateNode inputNode = new GenerateNode();
        inputNode.setLogInfo(1338697301, "www.taobao.com", "www.baidu.com", "auction_id", "shop_id", "ip", "mid",
                "uid", "sid", "cookie", 0);
        inputNode.addinTypeRef(1, 1, true, 2, 3);
        inputNode.addCapturedInfo("key1", "value1");
        inputNode.addSourceInfo("key2", 1);
        inputNode.addAccessUsefulExtra("adid", "adid");
        inputNode.setPageDuration(0);
        List<BytesWritable> values = new ArrayList<BytesWritable>();
        values.add(new BytesWritable(inputNode.build()));
        reduceDriver.withInput(key, values);

        GenerateNode outputNode = new GenerateNode();
        outputNode.setLogInfo(1338697301, "www.taobao.com", "www.baidu.com", "auction_id", "shop_id", "ip", "mid",
                "uid", "sid", "cookie", 0);
        outputNode.addinTypeRef(1, 1, true, 2, 3);
        outputNode.addTreeInfo(true, true, "0");
        outputNode.addAccessUsefulExtra("adid", "adid");
        outputNode.addAccessUsefulExtra("key1", "value1");
        outputNode.setPageDuration(0);
        String outdata = outputNode.buildtoString();

        reduceDriver.withOutput(new Text("1"), new Text(outdata));

        reduceDriver.runTest();
    }

}
