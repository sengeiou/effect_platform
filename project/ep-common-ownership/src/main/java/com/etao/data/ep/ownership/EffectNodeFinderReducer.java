package com.etao.data.ep.ownership;

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

import com.etao.lz.dw.util.TextPair;
import com.etao.lz.effect.HoloConfig;
import com.etao.lz.effect.exception.HoloConfigParserException;
import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;
import com.etao.data.ep.ownership.proto.LzEffectProtoUtil;

public class EffectNodeFinderReducer extends MapReduceBase implements
		Reducer<TextPair, BytesWritable, Text, Text> {

	// 建树最大结点数，默认2000
	private int maxNodeNum = 2000;

	// 计数器，记录已输入构树器中的记录条数
	private int nodeNum = 0;

	private EffectTreeNodeList treeNodeList;

	/**
	 * Reduce函数前的配置函数
	 * 
	 * @param conf
	 * @return
	 */
	public void configure(JobConf conf) {
		boolean isAll = false;
		boolean root_is_lp = false;
		
		// 获取输入参数
		if (conf.get("MAX_NODE_NUM") != null)
			maxNodeNum = Integer.parseInt(conf.get("MAX_NODE_NUM"));
		if (conf.get("IS_ALL") != null)
			isAll = Boolean.parseBoolean(conf.get("IS_ALL"));
		if (conf.get("ROOT_IS_LP") != null){
			root_is_lp = Boolean.parseBoolean(conf.get("ROOT_IS_LP"));
		}
		if (root_is_lp)
			isAll = root_is_lp;
		
		treeNodeList = new EffectTreeNodeList(isAll, root_is_lp);
		String conf_files = conf.get("config_paths");
		
		for(String conf_file:conf_files.split(",")){
			if(conf_file.length()==0){
				continue;
			}
			HoloConfig config = new HoloConfig();
			try {
				config.loadFile(conf_file);
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
			treeNodeList.addHoloConfig(config);
			
		}
	}

	
	/**
	 * MapReduce程序的reduce方法，完成树的构建和树结点的染色，默认最后输出染色的树结点
	 */
	public void reduce(TextPair key, Iterator<BytesWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		if (treeNodeList == null) {
			throw new IOException();
		}

		treeNodeList.setOutputParam(output, reporter);
		
		while (values.hasNext()) {
			BytesWritable nodeData = values.next();
			TreeNodeValue nodeValue = LzEffectProtoUtil.deserialize(Arrays
					.copyOf(nodeData.getBytes(), nodeData.getLength()));
			if (nodeValue == null) {
				continue;
			}
			
			treeNodeList.append(nodeValue);
			// 建树结点计数器
			nodeNum += 1;
			// 当同一个key包含的结点树大于maxNodeNum时，以maxNodeNum个结点构树，并输出建好的树
			if (nodeNum >= maxNodeNum) {
				treeNodeList.buildAllTrees();
				nodeNum = 0;
				treeNodeList.clear();
			}
		}
		// 输出最后一批树
		treeNodeList.buildAllTrees();
		treeNodeList.clear();
	}
}
