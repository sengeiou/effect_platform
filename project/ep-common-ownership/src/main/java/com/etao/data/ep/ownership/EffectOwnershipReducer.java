package com.etao.data.ep.ownership;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.xml.sax.SAXException;

import com.etao.lz.effect.HoloConfig;
import com.etao.lz.effect.exception.HoloConfigParserException;
import com.etao.data.ep.ownership.proto.LzEffectProtoUtil;
import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;
import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue.TypeRef;
import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue.TypeRef.TypePathInfo;
import com.etao.data.ep.ownership.util.EffectOwnershipStatusCounter;
import com.etao.lz.dw.util.TextPair;

public class EffectOwnershipReducer extends MapReduceBase implements
		Reducer<TextPair, BytesWritable, Text, Text> {

	// 同优先级来源效果归属规则 ( key: plan_id, value: first/last/equal/all )
	private HashMap<Integer, String> plan_map = new HashMap<Integer, String>();
	private HashMap<Integer, EffectOwnershipProcessBusinessLog> process_map = new HashMap<Integer, EffectOwnershipProcessBusinessLog>();

	public void configure(JobConf conf) {
		String conf_files = conf.get("config_paths");
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
			int plan_id = config.plan_id;
			String attr_calc = config.attr_calc_method;
			if (attr_calc == null) {
				return;
			}
			plan_map.put(plan_id, attr_calc);
		}
	}

	@Override
	public void reduce(TextPair key, Iterator<BytesWritable> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {

		if (plan_map.size() == 0) {
			throw new IOException();
		}
		process_map.clear();

		// 进入reduce的都是 auction_id, uid相同的，按照ts倒序排列
		while (values.hasNext()) {
			BytesWritable nodeData = values.next();
			TreeNodeValue value = LzEffectProtoUtil.deserialize(Arrays.copyOf(
					nodeData.getBytes(), nodeData.getLength()));
			if (value == null) {
				continue;
			}

			String auction_id = value.getAuctionId();
			// String shop_id = value.getShopId();

			if (value.getLogType() == 0) { // 访问日志
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_REDUCE_PV_TOTAL,
						1);
				for (TypeRef type_ref : value.getTypeRefList()) { // 处理多个plan情况

					int plan_id = type_ref.getPlanId();
					String attr_calc = plan_map.get(type_ref.getPlanId());
					EffectOwnershipAccessTreeNode node = processAccess(value,
							type_ref);

					if (auction_id == null || auction_id.length() == 0
							|| auction_id.equals("0")) {
						// 宝贝为空，直接输出此node
						for (String result : node.toStringList()) {
							output.collect(new Text(String.valueOf(plan_id)),
									new Text(result));
						}
					} else {
						// 有宝贝ID，进行处理
						EffectOwnershipProcessBusinessLog process = process_map
								.get(plan_id);
						if (process == null) {
							process = new EffectOwnershipProcessBusinessLog(
									output);
							process_map.put(plan_id, process);
						}

						process.appendAccessLog(node, attr_calc);
					}
				}

			} else if (value.getLogType() == 1) { // 交易日志
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_REDUCE_GMV_TOTAL,
						1);
				if (auction_id == null || auction_id.length() == 0
						|| auction_id.equals("0")) {
					reporter.incrCounter(
							EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_REDUCE_GMV_AUCTIONID_NULL,
							1);
					System.out.println("交易日志不应有没有auction_id情况");
					continue;
				} else {
					EffectOwnershipGmvTreeNode gmv_node = new EffectOwnershipGmvTreeNode(
							value);

					Iterator<EffectOwnershipProcessBusinessLog> it = process_map
							.values().iterator();
					while (it.hasNext()) {
						EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it
								.next();
						for (EffectOwnershipAccessTreeNode node : process
								.getNodes()) {
							// 单独输出一条成交日志，不和浏览汇合。
							gmv_node.CopyFromAccessTreeNode(node);
							gmv_node.calcGmvInfo();
							for (String result : gmv_node.toStringList()) {
								output.collect(
										new Text(String
												.valueOf(gmv_node.plan_id)),
										new Text(result));
							}
						}
					}
				}
			} else if (value.getLogType() == 2) { // 收藏日志
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_REDUCE_COLLECT_TOTAL,
						1);
				// 现在只计算宝贝收藏
				if (auction_id == null || auction_id.length() == 0
						|| auction_id.equals("0")) {
					reporter.incrCounter(
							EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_REDUCE_COLLECT_AUCTIONID_NULL,
							1);
					System.out.println("收藏日志不应有没有auction_id情况");
					continue;
				} else {
					EffectOwnershipCollectTreeNode collect_node = new EffectOwnershipCollectTreeNode(
							value);

					Iterator<EffectOwnershipProcessBusinessLog> it = process_map
							.values().iterator();
					while (it.hasNext()) {
						EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it
								.next();
						for (EffectOwnershipAccessTreeNode node : process
								.getNodes()) {
							// 单独输出一条收藏日志，不和浏览汇合。
							collect_node.CopyFromAccessTreeNode(node);
							collect_node.calcCollectInfo();
							for (String result : collect_node.toStringList()) {
								output.collect(
										new Text(String
												.valueOf(collect_node.plan_id)),
										new Text(result));
							}
						}
					}
				}

			} else if (value.getLogType() == 3) { // 购物车日志
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_REDUCE_CART_TOTAL,
						1);
				if (auction_id == null || auction_id.length() == 0
						|| auction_id.equals("0")) {
					reporter.incrCounter(
							EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_REDUCE_CART_AUCTIONID_NULL,
							1);
					System.out.println("购物车日志不应有没有auction_id情况");
					continue;
				} else {
					EffectOwnershipCartTreeNode cart_node = new EffectOwnershipCartTreeNode(
							value);

					Iterator<EffectOwnershipProcessBusinessLog> it = process_map
							.values().iterator();
					while (it.hasNext()) {
						EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it
								.next();
						for (EffectOwnershipAccessTreeNode node : process
								.getNodes()) {
							// 单独输出一条购物车日志，不和浏览汇合。
							cart_node.CopyFromAccessTreeNode(node);
							cart_node.calcCartInfo();
							for (String result : cart_node.toStringList()) {
								output.collect(
										new Text(String
												.valueOf(cart_node.plan_id)),
										new Text(result));
							}
						}
					}
				}

			} else {
				reporter.incrCounter(
						EffectOwnershipStatusCounter.EffectOwnershipStatus.EFFECT_OWNERSHIP_REDUCE_OTHER_LOG,
						1);
				continue;
			}
		}

		// 输出剩余的日志
		Iterator<EffectOwnershipProcessBusinessLog> it = process_map.values()
				.iterator();
		while (it.hasNext()) {
			EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it
					.next();
			process.clearNodes();
		}
	}

	private EffectOwnershipAccessTreeNode processAccess(TreeNodeValue value,
			TypeRef type_ref) {
		EffectOwnershipAccessTreeNode node = new EffectOwnershipAccessTreeNode(
				value);
		String attr_calc = plan_map.get(type_ref.getPlanId());
		node.setPlanInfo(type_ref, attr_calc);

		for (TypePathInfo path_info : type_ref.getPathInfoList()) {
			node.setPlanPathInfo(path_info);
		}
		return node;
	}
}
