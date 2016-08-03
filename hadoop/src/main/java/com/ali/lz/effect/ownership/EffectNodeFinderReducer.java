package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.xml.sax.SAXException;

import com.ali.lz.effect.exception.HoloConfigParserException;
import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.holotree.HoloSet;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.utils.Constants;

public class EffectNodeFinderReducer extends MapReduceBase implements Reducer<TextPair, BytesWritable, Text, Text> {

    // 建树最大结点数，默认2000
    private int maxNodeNum = 2000;

    private EffectTreeNodeList treeNodeList = null;

    private HoloSet holoKits = new HoloSet();

    public void configure(JobConf conf) {

        String confFiles = conf.get(Constants.CONFIG_FILE_PATH);

        for (String confFile : confFiles.split(",")) {
            if (confFile.length() == 0) {
                continue;
            }
            HoloConfig config = new HoloConfig();
            try {
                config.loadFile(confFile);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
                return;
            } catch (SAXException e) {
                e.printStackTrace();
                return;
            } catch (IOException e) {
                e.printStackTrace();
                return;
            } catch (HoloConfigParserException e) {
                e.printStackTrace();
                return;
            }
            Arrays.sort(config.lookahead);
            holoKits.addHoloConfig(config);
        }
        treeNodeList = new EffectTreeNodeList(holoKits, maxNodeNum);
    }

    /**
     * 完成树的构建和树结点的染色，默认最后输出染色的树结点
     */
    public void reduce(TextPair key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {

        treeNodeList.setOutputParam(output, reporter);

        while (values.hasNext()) {
            BytesWritable nodeData = values.next();
            TreeNodeValue nodeValue = LzEffectProtoUtil.deserialize(Arrays.copyOf(nodeData.getBytes(),
                    nodeData.getLength()));
            if (nodeValue == null) {
                reporter.incrCounter(EffectJobStatusCounter.FinderJobProcessStatus.PB_DESERIALIZE_ERROR, 1);
                continue;
            }

            treeNodeList.append(nodeValue);
        }
        treeNodeList.buildAllTrees();
        treeNodeList.clear();
    }

}
