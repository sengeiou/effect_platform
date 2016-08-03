package com.ali.lz.effect.ownership;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ali.lz.effect.extendutils.MapReduceDriverUtil;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.utils.Constants;

public class EffectNodeFinderMapReduceTest {

    private MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        EffectNodeFinderMapper mapper = new EffectNodeFinderMapper();
        EffectNodeFinderReducer reducer = new EffectNodeFinderReducer();

        mapReduceDriver = new MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.setKeyGroupingComparator(new TextPair.RealComparator());
    }

    @Test
    public void testspm() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/report_26.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/treebuilder_spm_20120618_1.log", "26",
                "src/test/resources/treebuilder_spm_20120618_1.output", m_conf);
    }

    @Test
    public void testspm2() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/report_26.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/treebuilder_spm_20120618_2.log", "26",
                "src/test/resources/treebuilder_spm_20120618_2.output", m_conf);
    }

    @Test
    public void testadid() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/report_49.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/tb_market_id_input1.log", "49",
                "src/test/resources/tb_market_id_output1.output", m_conf);
    }

    @Test
    public void testMultiReport1() {

        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/report_49.xml,src/test/resources/report_26.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/tb_market_id_input1.log", "49",
                "src/test/resources/tb_market_id_output1.output", m_conf);
    }

    @Test
    public void testb2b() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/b2c_source.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/treebuilder_b2b_20120731_1.log", "1",
                "src/test/resources/treebuilder_b2b_20120731_1.output", m_conf);
    }

    @Test
    public void testb2b2() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/b2c_source.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/treebuilder_b2b_20120731_2.log", "1",
                "src/test/resources/treebuilder_b2b_20120731_2.output", m_conf);
    }

    @Test
    public void testLookahead() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/report_149.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/lz_fact_ep_browse_log_testdata", "149",
                "src/test/resources/lz_fact_ep_browse_log_output", m_conf);
    }

    @Test
    public void testetao() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/report_6.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/treebuilder_etao_20120824_1.log", "6",
                "src/test/resources/treebuilder_etao_20120824_1.output", m_conf);
    }

    /**
     * 含效果页折叠场景
     */
    @Test
    public void testetui() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/report_etui.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/lz_etui_20121205.log", "1",
                "src/test/resources/lz_etui_20121205.output", m_conf);
    }

    @Test
    public void testKoolbaoAd() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/ltj_ep_config_ad_1000.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/koolbao_ad.input", "1000",
                "src/test/resources/koolbao_ad.output", m_conf);
    }

    /**
     * 测试路径中黄金令箭标记继承方法
     */
    @Test
    public void testhjlj1() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/hjlj_effect/report_hjlj.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/hjlj_effect/inherit_hjlj_logkey.input", "1",
                "src/test/resources/hjlj_effect/inherit_hjlj_logkey.output", m_conf);
    }

    /**
     * 测试根节点黄金令箭标记方法
     */
    @Test
    public void testhjlj2() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/hjlj_effect/report_hjlj.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/hjlj_effect/inherit_hjlj_logkey1.input", "1",
                "src/test/resources/hjlj_effect/inherit_hjlj_logkey1.output", m_conf);
    }

    /**
     * 测试U站效果页case
     */
    @Test
    public void testUz1() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/uz_effect/report_uz.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/uz_effect/uz_log1.input", "1",
                "src/test/resources/uz_effect/uz_log1.output", m_conf);
    }
    
    /**
     * 测试U站效果页折叠case
     */
    @Test
    public void testUz2() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/uz_effect/report_uz.xml");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/uz_effect/uz_log2.input", "1",
                "src/test/resources/uz_effect/uz_log2.output", m_conf);
    }

}
