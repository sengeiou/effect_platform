package com.ali.lz.effect.ownership.wireless;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.extendutils.MapReduceDriverUtil;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.utils.Constants;

public class EffectWirelessTreeMapReduceTest {
    private MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        EffectWirelessTreeMapper mapper = new EffectWirelessTreeMapper();
        EffectWirelessTreeReducer reducer = new EffectWirelessTreeReducer();

        mapReduceDriver = new MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.setKeyGroupingComparator(new TextPair.RealComparator());
    }

    /**
     * 测试点： 活动页引导的三类坑位染色标记情况
     */
    @Test
    public void testWirelessTreePitId() {

        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/wireless_config/config_file");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/wireless_data/wireless_tree_pitid.src", "",
                "src/test/resources/wireless_data/wireless_tree_pitid.output", m_conf);

    }

    /**
     * 一阳指配置的活动页url可能被重新组装然后记录到日志中，因此需要还原真实的活动页url来匹配活动id 测试点：
     * 活动页url/refer修正后是否被正确匹配
     */
    @Test
    public void testWirelessTreeEffectUrlFix() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/wireless_config/config_file");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/wireless_data/wireless_tree_effect_url_fix.src", "",
                "src/test/resources/wireless_data/wireless_tree_effect_url_fix.output", m_conf);

    }

    /**
     * 测试点: 活动规则之间有包含关系时染色情况
     */
    @Test
    public void testWirelessTreeMultiRulesPitId() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/wireless_config/config_file");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/wireless_data/wireless_tree_multirules.src", "",
                "src/test/resources/wireless_data/wireless_tree_multirules.output", m_conf);

    }

}
