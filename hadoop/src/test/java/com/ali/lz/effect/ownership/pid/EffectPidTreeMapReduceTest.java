package com.ali.lz.effect.ownership.pid;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.extendutils.MapReduceDriverUtil;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.ownership.pid.EffectPidTreeMapper;
import com.ali.lz.effect.ownership.pid.EffectPidTreeReducer;
import com.ali.lz.effect.utils.Constants;

public class EffectPidTreeMapReduceTest {

    private MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        EffectPidTreeMapper mapper = new EffectPidTreeMapper();
        EffectPidTreeReducer reducer = new EffectPidTreeReducer();

        mapReduceDriver = new MapReduceDriverUtil<Object, Text, TextPair, BytesWritable, Text, Text>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
        mapReduceDriver.setKeyGroupingComparator(new TextPair.RealComparator());
    }

    /**
     * 测试点： 活动页引导的三类坑位染色标记情况
     */
    @Test
    public void testPidTreePitId() {
        Map<String, String> m_conf = new HashMap<String, String>();
        m_conf.put(Constants.CONFIG_FILE_PATH, "src/test/resources/pid_channel_conf");
        mapReduceDriver.runSingleKeyConfTest("src/test/resources/pid_tree.src", "",
                "src/test/resources/pid_tree.output", m_conf);

    }

}
