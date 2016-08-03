package com.ali.lz.effect.ownership.pid;

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
import com.ali.lz.effect.proto.LzEffectPidProtoUtil;
import com.ali.lz.effect.proto.LzEffectPidProto.PidNodeValue;

public class EffectPidTreeReducer extends MapReduceBase implements Reducer<TextPair, BytesWritable, Text, Text> {

    // 建树最大结点数，默认2000
    private int maxNodeNum = 2000;

    // 计数器，记录已输入构树器中的记录条数
    private int nodeNum = 0;
    private boolean hasChannelVisit = false;
    private ArrayList<PidNodeValue> treeNodeList = new ArrayList<PidNodeValue>(maxNodeNum);

    @Override
    public void reduce(TextPair key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {
        // TODO Auto-generated method stub
        // 建树过程
        HoloTreeBuilder builder = new HoloTreeBuilder(new HoloConfig());
        builder.setDoPathMatch(false);

        while (values.hasNext()) {
            BytesWritable nodeData = values.next();
            PidNodeValue nodeValue = LzEffectPidProtoUtil.deserialize(Arrays.copyOf(nodeData.getBytes(),
                    nodeData.getLength()));
            if (nodeValue == null) {
                continue;
            }
            treeNodeList.add(nodeValue);
            if (nodeValue.getIsEffectPage() || nodeValue.getReferIsEffectPage())
                hasChannelVisit = true;
            // 建树结点计数器
            nodeNum += 1;
            // 当同一个key包含的结点树大于maxNodeNum时，以maxNodeNum个结点构树，并输出建好的树
            if (nodeNum >= maxNodeNum) {
                if (hasChannelVisit) {
                    for (PidNodeValue node : treeNodeList) {
                        PTLogEntry logEntry = EffectPidTreeUtil.genLogEntry(node);
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
            for (PidNodeValue node : treeNodeList) {
                PTLogEntry logEntry = EffectPidTreeUtil.genLogEntry(node);
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
                if (parent != null) {
                    // 标记频道页二跳信息
                    PTLogEntry parentLogEntry = parent.getPtLogEntry();
                    if ((Boolean) parentLogEntry.get("is_channel_lp")) {
                        // 此类标记仅用于计算频道页二跳pv, uv
                        node.getPtLogEntry().put("refer_is_channel_lp", true);
                        node.getPtLogEntry().put("refer_src_refer", parentLogEntry.get("src_refer"));
                        node.getPtLogEntry().put("refer_src_refer_type", parentLogEntry.get("src_refer_type"));
                    }

                    // 标记List坑位引导的宝贝页
                    if (EffectPitType.LIST_PIT == (Integer) parentLogEntry.get("pit_id")
                            && ((String) parentLogEntry.get("auction_id")).length() <= 1
                            && ((String) node.getPtLogEntry().get("auction_id")).length() > 1) {

                        node.getPtLogEntry().put("pit_id", EffectPitType.LIST_PIT);
                        node.getPtLogEntry().put("pit_detail", (String) parentLogEntry.get("pit_detail"));
                        reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_LIST_PIT_ITEMS, 1);
                    }

                    // 染色宝贝页或相同频道页继承最近的频道lp页的来源信息
                    int channelId = (Integer) node.getPtLogEntry().get("channel_id");
                    if (!(Boolean) node.getPtLogEntry().get("is_channel_lp")
                            && ((Integer) node.getPtLogEntry().get("pit_id") > 0 || channelId > 0)) {
                        while (parent != null && !(Boolean) parentLogEntry.get("is_channel_lp")) {
                            parent = parent.getParent();
                            if (parent != null)
                                parentLogEntry = parent.getPtLogEntry();
                        }
                        if (parent != null) {
                            node.getPtLogEntry().put("pid", parentLogEntry.get("pid"));
                            node.getPtLogEntry().put("pub_id", parentLogEntry.get("pub_id"));
                            node.getPtLogEntry().put("site_id", parentLogEntry.get("site_id"));
                            node.getPtLogEntry().put("adzone_id", parentLogEntry.get("adzone_id"));
                            node.getPtLogEntry().put("channel_id", parentLogEntry.get("channel_id"));
                            node.getPtLogEntry().put("src_refer", parentLogEntry.get("src_refer"));
                            node.getPtLogEntry().put("src_refer_type", parentLogEntry.get("src_refer_type"));
                        }
                    }
                } else {
                    // 处理特殊的根节点
                    if ((Integer) node.getPtLogEntry().get("pit_id") > 0) {
                        node.getPtLogEntry().put("channel_id", node.getPtLogEntry().get("refer_channel_id"));
                    }
                    if (((Integer) node.getPtLogEntry().get("channel_id")) > 0) {
                        node.getPtLogEntry().put("is_channel_lp", true);
                    }
                }
                PidNodeValue nodeValue = EffectPidTreeUtil.genPidNodeBuilder(node).build();
                output.collect(new Text(""), new Text(EffectPidTreeUtil.toString(nodeValue)));
            }
        }
    }
}
