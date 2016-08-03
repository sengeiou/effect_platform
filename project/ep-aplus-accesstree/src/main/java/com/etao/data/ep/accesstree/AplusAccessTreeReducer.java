package com.etao.data.ep.accesstree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.SortedMap;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.etao.data.ep.accesstree.proto.AplusAccessTreeNodeProtoUtil;
import com.etao.data.ep.accesstree.proto.AplusAccessTreeNodeProto.AplusAccessTreeNodeValue;
import com.etao.data.ep.accesstree.util.AplusAccessTreeStatusCounter;
import com.etao.lz.effect.HoloConfig;
import com.etao.lz.effect.HoloTreeBuilder;
import com.etao.lz.effect.HoloTreeNode;
import com.etao.lz.effect.PTLogEntry;
import com.etao.lz.dw.util.Constants;
import com.etao.lz.dw.util.TextPair;

public class AplusAccessTreeReducer extends MapReduceBase implements
		Reducer<TextPair, BytesWritable, Text, Text> {
    
    private final String VERSION = "2.0.0";
    // 建树最大结点数，默认1000
    private int maxNodeNum = 1000;

    // 计数器，记录已输入构树器中的记录条数
    private int nodeNum = 0;
    private ArrayList<AplusAccessTreeNodeValue> treeNodeList = new ArrayList<AplusAccessTreeNodeValue>(maxNodeNum);

    @Override
    public void reduce(TextPair key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {
        // TODO Auto-generated method stub
        // 建树过程
        HoloTreeBuilder builder = new HoloTreeBuilder(new HoloConfig());
        builder.setDoPathMatch(false);

        while (values.hasNext()) {
            BytesWritable nodeData = values.next();
			AplusAccessTreeNodeValue nodeValue = AplusAccessTreeNodeProtoUtil
					.deserialize(Arrays.copyOf(nodeData.getBytes(),
							nodeData.getLength()));
			if (nodeValue == null) {
				reporter.incrCounter(
						AplusAccessTreeStatusCounter.AccessTreeBuilderStatus.APLUS_HOLOTREE_VALUE_NULL,
						1);
				continue;
			}
            treeNodeList.add(nodeValue);
            // 建树结点计数器
            nodeNum += 1;
            // 当同一个key包含的结点树大于maxNodeNum时，以maxNodeNum个结点构树，并输出建好的树
            if (nodeNum >= maxNodeNum) {
                
                for (AplusAccessTreeNodeValue node : treeNodeList) {            
                    PTLogEntry logEntry = AplusAccessTreeNodeProtoUtil
					.genLogEntry(node);
                    builder.appendLog(logEntry);
                }
                ColorizeTreeNode(output, reporter, builder);
                // 清空建树器，初始化计数器，开始另一轮建树
                builder.flush();
                
                nodeNum = 0;
                treeNodeList.clear();
            }
        }
        // 输出最后一批树
        for (AplusAccessTreeNodeValue node : treeNodeList) {
            PTLogEntry logEntry = AplusAccessTreeNodeProtoUtil
					.genLogEntry(node);
            builder.appendLog(logEntry);
        }
        ColorizeTreeNode(output, reporter, builder);
        
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
				String index_root_path = node.getSerialRootPath().replace(".",
						Constants.CTRL_B);
				if (index_root_path.indexOf(Constants.CTRL_B) == -1) {
					node.getPtLogEntry().put("tree_id", index_root_path);
					node.getPtLogEntry().put("node_id", index_root_path);
					node.getPtLogEntry().put("parent_id", "-1");
					node.getPtLogEntry().put("parent_list", "");
				} else if (index_root_path.indexOf(Constants.CTRL_B) > 0) {
					String tree_id = index_root_path.substring(0,
							index_root_path.indexOf(Constants.CTRL_B));
					String parentList = index_root_path.substring(0,
							index_root_path.lastIndexOf(Constants.CTRL_B));
					String node_id = index_root_path.substring(index_root_path
							.lastIndexOf(Constants.CTRL_B) + 1);
					String parent_id = parentList.substring(parentList
							.lastIndexOf(Constants.CTRL_B) + 1);
					node.getPtLogEntry().put("tree_id", tree_id);
					node.getPtLogEntry().put("node_id", node_id);
					node.getPtLogEntry().put("parent_id", parent_id);
					node.getPtLogEntry().put("parent_list", parentList);
				} else {
					reporter.incrCounter(
							AplusAccessTreeStatusCounter.AccessTreeBuilderStatus.APLUS_HOLOTREE_NODEID_ERROR,
							1);
					continue;
				}

				node.getPtLogEntry().put("version", VERSION);
				node.getPtLogEntry().put("proxy", "");
				node.getPtLogEntry().put("pvtime",
						node.getPtLogEntry().get("ts"));
				String revised_refer = ((Boolean) node.getPtLogEntry().get(
						"refer_revised") ? ((String) node.getPtLogEntry().get(
						"refer_url")) : "0");
				node.getPtLogEntry().put("revised_refer",
						"".equals(revised_refer) ? "0" : revised_refer);

				AplusAccessTreeNodeValue nodeValue = AplusAccessTreeNodeProtoUtil
						.genBuilder(node).build();
				output.collect(new Text(""), new Text(
						AplusAccessTreeNodeProtoUtil.toString(nodeValue)));

				node.getPtLogEntry().clear();
			}
        }
    }
    
}
