package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ali.lz.effect.extendutils.MRUnitTools;
import com.ali.lz.effect.extendutils.MapDriverUtil;
import com.ali.lz.effect.extendutils.MapReduceDriverUtil;
import com.ali.lz.effect.extendutils.ReduceDriverUtil;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.Constants.EGroupingKeyType;
import com.ali.lz.effect.utils.StringUtil;

public class EffectOwnershipTest {

    private MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text> mapReduceDriver;
    private MapDriverUtil<Object, Text, TextPair, BytesWritable> accessMapDriver;
    private MapDriverUtil<Object, Text, TextPair, BytesWritable> gmvMapDriver;
    private ReduceDriverUtil<TextPair, BytesWritable, Text, Text> reduceDriver;

    EffectOwnershipMapper.HoloTreeMapper accessMapper = new EffectOwnershipMapper.HoloTreeMapper();
    EffectOwnershipMapper.GmvMapper gmvMapper = new EffectOwnershipMapper.GmvMapper();
    EffectOwnershipReducer reducer = new EffectOwnershipReducer();

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
    public void testAccessMapper1() {
        // 测试当type_ref_infos为空时情况
        Configuration conf = new Configuration();
        conf.set(Constants.CONFIG_FILE_PATH, "src/test/resources/report_96.xml");
        mapReduceDriver.setConfiguration(conf);
        String[] in = { "index_root_path", "1338697301", "", "url", "refer_url", "m_id", "s_id", "ip", "cookie",
                "shop_id", "auction_id", "uid", "adid", "dim_id" };
        mapReduceDriver.withInput(new LongWritable(), new Text(StringUtil.join(in, Constants.CTRL_A)));
        mapReduceDriver.runTest();
    }

    @Test
    public void testAccessMapper2() {
        // 测试当type_ref_infos为空时情况
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/report_etui.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/lz_etui_owner_20121205.log", "1",
                "src/test/resources/lz_etui_owner_20121205.output", m_conf);
    }
    
    @Test
    public void testAccessMapperUz() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/uz_effect/report_uz.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/uz_effect/uz_log2.output", "1",
                "src/test/resources/uz_effect/uz_log2_ownership.output", m_conf);
    }

    @Test
    public void testKoolbaoOwnership() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/ltj_ep_config_shop_1.xml");
        List<Pair<TextPair, BytesWritable>> mapOutputs = new ArrayList<Pair<TextPair, BytesWritable>>();
        
        Configuration mapConf = new Configuration();
        mapConf.set("gmv_ownership", "true");
        accessMapDriver.setConfiguration(mapConf);
        accessMapDriver.setMultiInputsfromFile("src/test/resources/koolbao_acookie_tree1");

        gmvMapDriver.setMultiInputsfromFile("src/test/resources/koolbao_trade1");

        try {
            mapOutputs.addAll(accessMapDriver.run());
            mapOutputs.addAll(gmvMapDriver.run());
            TextPair key = null;
            List<BytesWritable> values = new ArrayList<BytesWritable>();
            for (Pair<TextPair, BytesWritable> pair : mapOutputs) {
                if (!pair.getFirst().getFirst().toString().startsWith(EGroupingKeyType.ACCESS_LOG.getPrefix())) {
                    key = pair.getFirst();
                    values.add(pair.getSecond());
                }
            }
            reduceDriver.setConfiguration(MRUnitTools.getConfigurationfromMap(m_conf));
            reduceDriver.runSingleKeyConfTest(key, values, "2", "src/test/resources/koolbao_acookie_ownership");

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
