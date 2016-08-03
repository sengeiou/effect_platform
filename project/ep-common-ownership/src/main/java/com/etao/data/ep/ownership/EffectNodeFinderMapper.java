package com.etao.data.ep.ownership;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.xml.sax.SAXException;

import com.etao.lz.effect.HoloConfig;
import com.etao.lz.effect.PTLogEntry;
import com.etao.lz.effect.URLMatcher;
import com.etao.lz.effect.exception.HoloConfigParserException;
import com.etao.lz.effect.exception.URLMatcherException;
import com.etao.lz.dw.util.TextPair;
import com.etao.lz.dw.util.Constants;
import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;
import com.etao.data.ep.ownership.proto.LzEffectProtoUtil;
import com.etao.data.ep.ownership.util.EffectOwnershipStatusCounter;

public class EffectNodeFinderMapper extends MapReduceBase implements
		Mapper<Object, Text, TextPair, BytesWritable> {

	private class PlanMapValue {
		public int user_id = -1;
		public URLMatcher matcher = null;
	}

	private String tree_split = "none";
	private HashMap<Integer, PlanMapValue> plan_map = new HashMap<Integer, PlanMapValue>();
	private static int ACCESS_LOG_COLUMN_NUM = 11;

	@Override
	public void configure(JobConf conf) {
		// 获取配置xml文件名
		String conf_files = conf.get("config_paths");
		// 创建matcher类
		for (String conf_file : conf_files.split(",")) {
			if (conf_file.length() == 0) {
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
			PlanMapValue value = new PlanMapValue();
			URLMatcher matcher = new URLMatcher(config);
			// 得到当前配置的rule_id
			int plan_id = config.plan_id;
			value.user_id = config.analyzer_id;
			value.matcher = matcher;
			// 切分树的属性，控制计算范围。TODO
			tree_split = config.tree_split_method;
			// 加入到plan_map中
			plan_map.put(plan_id, value);
		}
	}

	public void map(Object key, Text value,
			OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
			throws IOException {

		if (plan_map.size() == 0) {
			throw new IOException();
		}

		String line = value.toString();
		String[] fields = line.split(Constants.CTRL_A, -1);
		// 判断输入字段长度
		if (fields.length < ACCESS_LOG_COLUMN_NUM) {
			reporter.incrCounter(
					EffectOwnershipStatusCounter.BuilderTreeStatus.BUILDER_TREE_MAP_COLUMN_NUM_ERROR,
					1);
			System.out.println("数据错误");
			return;
		}

		// 声明protocol buffer Builder
		TreeNodeValue.KeyValueI.Builder keyValueI_builder = TreeNodeValue.KeyValueI
				.newBuilder();
		TreeNodeValue.KeyValueS.Builder keyValueS_builder = TreeNodeValue.KeyValueS
				.newBuilder();
		TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef
				.newBuilder();
		TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

		// 写入日志内容参数
		builder.setTs(Integer.parseInt(fields[0].trim()));
		builder.setUrl(fields[1]);
		builder.setRefer(fields[2]);
		builder.setShopId(fields[3]);
		builder.setAuctionId(fields[4]);
		builder.setUserId(fields[5]);
		builder.setCookie(fields[6]);
		builder.setSession(fields[7]);
		builder.setCookie2(fields[8]);

		// 创建用来做match操作的map结构
		Map<String, String> match_map = new HashMap<String, String>();
		match_map.put("url", builder.getUrl());
		match_map.put("refer_url", builder.getRefer());

		String[] useful_extra = fields[9].split(Constants.CTRL_B, -1);
		for (String field : useful_extra) {
			String[] keyValue = field.split(Constants.CTRL_C, -1);
			if (keyValue.length != 2) {
				continue;
			}
			keyValueS_builder.setKey(keyValue[0]);
			keyValueS_builder.setValue(keyValue[1]);
			builder.addAccessUsefulExtra(keyValueS_builder);
			match_map.put(keyValue[0], keyValue[1]);
		}
		builder.setAccessExtra(fields[10]);

		// 写入type ref信息
		for (Map.Entry<Integer, PlanMapValue> p_entry : plan_map.entrySet()) {
			PlanMapValue m_value = p_entry.getValue();
			PTLogEntry result;
			try {
				result = m_value.matcher.grep(match_map);

				type_builder.setAnalyzerId(m_value.user_id);
				type_builder.setPlanId(p_entry.getKey());
				type_builder.setPtype(result.getPType());
				type_builder.setRtype(result.getRType());
				type_builder.setIsMatched(result.matched());

				for (Map.Entry<String, Object> captured : result.entrySet()) {
					if (captured.getKey().equals("ali_corp")) {
						builder.setAliCorp((Integer) captured.getValue());
					} else {
						keyValueS_builder.setKey(captured.getKey());
						keyValueS_builder
								.setValue((String) captured.getValue());
						type_builder.addCapturedInfo(keyValueS_builder);
					}
				}
				for (Map.Entry<String, Integer> source : result.getSourceType()
						.entrySet()) {
					keyValueI_builder.setKey(source.getKey());
					keyValueI_builder.setValue(source.getValue());
					type_builder.addSourceInfo(keyValueI_builder);
				}

			} catch (URLMatcherException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			builder.addTypeRef(type_builder);
		}

		// 根据路径拆分规则判断是否要拆分
		Boolean b_out = true;
		if (tree_split.indexOf("ali:") >= 0) {
			switch (builder.getAliCorp()) {
			case 1:
				b_out = tree_split.indexOf("etao") == -1 ? false : true;
			case 2:
				b_out = tree_split.indexOf("taobao") == -1 ? false : true;
			case 3:
				b_out = tree_split.indexOf("tmall") == -1 ? false : true;
			case 4:
				b_out = tree_split.indexOf("jhs") == -1 ? false : true;
			default:
				b_out = true;
			}
		}

		// 按规则输出
		if (b_out) {
			// TreeNode build
			TreeNodeValue node = builder.build();
			if (node == null) {
				return;
			}
			// 序列化为二进制文本
			byte[] data = LzEffectProtoUtil.serialize(node);

			Text sessionId = new Text(node.getCookie() + "_"
					+ node.getSession());
			Text timestamp = new Text(String.valueOf(node.getTs()));
			output.collect(new TextPair(sessionId, timestamp),
					new BytesWritable(data));
		}
	}
}
