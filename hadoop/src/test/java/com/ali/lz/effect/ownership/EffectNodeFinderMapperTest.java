package com.ali.lz.effect.ownership;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.ownership.EffectNodeFinderMapper;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;

public class EffectNodeFinderMapperTest {
    MapDriver<Object, Text, TextPair, BytesWritable> m;

    @Before
    public void setUp() {
        EffectNodeFinderMapper mapper = new EffectNodeFinderMapper();
        m = new MapDriver<Object, Text, TextPair, BytesWritable>();
        m.setMapper(mapper);
        Configuration conf = new Configuration();
        conf.set("config_paths", ClassLoader.getSystemResource("report_26.xml").getFile());
        m.setConfiguration(conf);
    }

    @Test
    public void test2() {
        m.withInput(
                new Text(""),
                new Text(
                        "1340012598urlrefer_url101704902oQeHBkTHfx8CAULF9HpKkfTf5755310280783825455101704902111.3.85.155Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; MATP; Media Center PC 6.0;  QIHU 360EE)-112_2012061817431820120618atpanel"));

        TreeNodeValue.KeyValueI.Builder keyValueI_builder = TreeNodeValue.KeyValueI.newBuilder();
        TreeNodeValue.KeyValueS.Builder keyValueS_builder = TreeNodeValue.KeyValueS.newBuilder();
        TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef.newBuilder();
        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

        builder.setTs(1340012598);
        builder.setUrl("url");
        builder.setRefer("refer_url");
        builder.setShopId("");
        builder.setAuctionId("");
        builder.setUserId("101704902");
        builder.setCookie("oQeHBkTHfx8CAULF9HpKkfTf");
        builder.setSession("5755310280783825455");
        builder.setVisitId("101704902");
        builder.setAliCorp(0);
        builder.setAccessExtra("111.3.85.155Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; MATP; Media Center PC 6.0;  QIHU 360EE)-112_20120618174318");

        type_builder.setAnalyzerId(4);
        type_builder.setPlanId(26);
        type_builder.setPtype(0);
        type_builder.setRtype(0);
        type_builder.setIsMatched(false);

        keyValueI_builder.setKey("adid");
        keyValueI_builder.setValue(2);
        type_builder.addSourceInfo(keyValueI_builder);

        builder.addTypeRef(type_builder);
        TreeNodeValue node = builder.build();
        byte[] data = LzEffectProtoUtil.serialize(node);

        Text sessionId = new Text(node.getCookie());
        Text timestamp = new Text(String.valueOf(node.getTs()));
        m.withOutput(new TextPair(sessionId, timestamp), new BytesWritable(data));

        m.runTest();
    }
}
