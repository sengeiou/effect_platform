package com.ali.lz.effect.ownership.wireless;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.holotree.HoloTreeBuilder;
import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.ownership.pid.EffectPitType;
import com.ali.lz.effect.proto.LzEffectWirelessProtoUtil;
import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessTreeNodeValue;
import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessTreeNodeValue.PlanProperty;

public class EffectWirelessTreeReducer extends MapReduceBase implements Reducer<TextPair, BytesWritable, Text, Text> {

    // 建树最大结点数，默认2000
    private int maxNodeNum = 2000;

    // 计数器，记录已输入构树器中的记录条数
    private int nodeNum = 0;
    private boolean hasChannelVisit = false;
    private ArrayList<WirelessTreeNodeValue> treeNodeList = new ArrayList<WirelessTreeNodeValue>(maxNodeNum);

    @Override
    public void reduce(TextPair key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {
        // 建树过程
        HoloTreeBuilder builder = new HoloTreeBuilder(new HoloConfig());
        builder.setDoPathMatch(false);

        while (values.hasNext()) {
            BytesWritable nodeData = values.next();
            WirelessTreeNodeValue nodeValue = LzEffectWirelessProtoUtil.deserializeWirelessTreeNodeValue(Arrays.copyOf(
                    nodeData.getBytes(), nodeData.getLength()));
            if (nodeValue == null) {
                continue;
            }
            treeNodeList.add(nodeValue);
            for (PlanProperty property : nodeValue.getPlanPropertiesList()) {
                if (property.getIsEffectPage() || property.getReferIsEffectPage())
                    hasChannelVisit = true;
            }
            // 建树结点计数器
            nodeNum += 1;
            // 当同一个key包含的结点树大于maxNodeNum时，以maxNodeNum个结点构树，并输出建好的树
            if (nodeNum >= maxNodeNum) {
                if (hasChannelVisit) {
                    for (WirelessTreeNodeValue node : treeNodeList) {
                        PTLogEntry logEntry = EffectWirelessTreeUtil.genLogEntry(node);
                        builder.appendLog(logEntry);
                    }
                    ColorizeTreeNode(output, reporter, builder);
                    // 清空建树器，初始化计数器，开始另一轮建树
                    builder.flush();
                }
                hasChannelVisit = false;
                nodeNum = 0;
                treeNodeList.clear();
            }
        }

        // 输出最后一批树
        if (hasChannelVisit) {
            for (WirelessTreeNodeValue node : treeNodeList) {
                PTLogEntry logEntry = EffectWirelessTreeUtil.genLogEntry(node);
                builder.appendLog(logEntry);
            }
            ColorizeTreeNode(output, reporter, builder);
        }
        hasChannelVisit = false;
        nodeNum = 0;
        treeNodeList.clear();

        // 清空建树器，初始化计数器，开始另一轮建树
        builder.flush();
    }

    private void ColorizeTreeNode(OutputCollector<Text, Text> output, Reporter reporter, HoloTreeBuilder builder)
            throws IOException {

        // 循环遍历树并输出每棵树的结点
        for (SortedMap<Long, HoloTreeNode> holoTree : builder.getCurrentTrees()) {
            Iterator<HoloTreeNode> it = holoTree.values().iterator();
            while (it.hasNext()) {
                // 轮询树中的每个树结点
                HoloTreeNode node = it.next();
                HoloTreeNode parent = node.getParent();
                // 标记List坑位引导的宝贝页
                if (parent != null && ((String) parent.getPtLogEntry().get("auction_id")).length() <= 1
                        && ((String) node.getPtLogEntry().get("auction_id")).length() > 1) {
                    for (PlanProperty parentProperty : (List<PlanProperty>) parent.getPtLogEntry().get(
                            "plan_properties")) {
                        if (String.valueOf(EffectPitType.LIST_PIT).equals(parentProperty.getPitId())) {
                            PlanProperty.Builder propertyBuilder = PlanProperty.newBuilder();
                            propertyBuilder.setPitId(String.valueOf(EffectPitType.LIST_PIT));
                            propertyBuilder.setPitDetail(parentProperty.getPitDetail());
                            propertyBuilder.setPlanId(parentProperty.getPlanId());
                            List<PlanProperty> properties = null;
                            if (((List) node.getPtLogEntry().get("plan_properties")).isEmpty()) {
                                properties = new ArrayList<PlanProperty>();

                            } else {
                                properties = (ArrayList<PlanProperty>) node.getPtLogEntry().get("plan_properties");
                            }
                            properties.add(propertyBuilder.build());
                            node.getPtLogEntry().put("plan_properties", properties);
                            reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_LIST_PIT_ITEMS, 1);
                        }
                    }
                }
                WirelessTreeNodeValue nodeValue = EffectWirelessTreeUtil.genWirelessTreeNodeBuilder(node).build();
                // output.collect(new Text(""), new
                // Text(EffectWirelessTreeUtil.toString(nodeValue)));
                EffectWirelessTreeUtil.output(nodeValue, output);
            }
        }
    }
}
