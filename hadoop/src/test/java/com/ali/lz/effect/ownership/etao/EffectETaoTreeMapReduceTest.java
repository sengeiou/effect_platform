package com.ali.lz.effect.ownership.etao;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.extendutils.MapReduceDriverUtil;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.utils.Constants;

public class EffectETaoTreeMapReduceTest {
    private MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        EffectETaoTreeMapper mapper = new EffectETaoTreeMapper();
        EffectETaoTreeReducer reducer = new EffectETaoTreeReducer();

        mapReduceDriver = new MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.setKeyGroupingComparator(new TextPair.RealComparator());
    }

    /**
     * 测试点：根节点 根节点分为宝贝页/非宝贝页(url=etao/refer=etao, url=etao/refer!=etao,
     * url!=etao/refer=etao)
     */
    @Test
    public void testETaoTreeRoot() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/etao_channel_conf");
        mapReduceDriver.runMultiKeysConfTest("src/test/resources/etao_tree_rootnode.src",
                "src/test/resources/etao_tree_rootnode.output", m_conf);
    }

    /**
     * 测试点：宝贝页继承channel_src case: pk channel页 -> pk channel页 -> detail.etao.com
     * -> etao ipv
     */
    @Test
    public void testETaoTreeChannelSrc() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/etao_channel_conf");
        mapReduceDriver.runMultiKeysConfTest("src/test/resources/etao_tree_20120919_1.src",
                "src/test/resources/etao_tree_20120919_1.output", m_conf);
    }

    /**
     * 测试点：lp页面标识规则 case: tmall.com -> detail.etao.com -> ipv1/ipv2/ipv3
     */
    @Test
    public void testETaoTreeLpInfo() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/etao_channel_conf");
        mapReduceDriver.runMultiKeysConfTest("src/test/resources/etao_tree_20120830.src",
                "src/test/resources/etao_tree_20120830.output", m_conf);
    }

    @Test
    public void testETaoTreeLpSrc() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/etao_channel_conf");
        mapReduceDriver.runMultiKeysConfTest("src/test/resources/etao_tree_lpsrc.src",
                "src/test/resources/etao_tree_lpsrc.output", m_conf);

    }

}
