package com.etao.data.ep.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.etao.lz.effect.HoloConfig;
import com.etao.lz.effect.HoloTreeBuilder;
import com.etao.lz.effect.HoloTreeNode;
import com.etao.lz.effect.PTLogEntry;
import com.etao.lz.effect.rule.RuleSet;
import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;
import com.etao.data.ep.ownership.proto.LzEffectProtoUtil;
import com.etao.data.ep.ownership.util.EffectOwnershipStatusCounter;
import com.etao.lz.dw.util.Constants;
import com.etao.lz.dw.util.StringUtil;

public class EffectTreeNodeList {
	private OutputCollector<Text, Text> output = null;
	private Reporter reporter = null;

	private boolean isAll = false; // 判断是否节点全部输出
	private boolean root_is_lp = false; // 判断是否把所有的根节点认为是效果页（主要是B2C来源分析需要这种逻辑）

	private List<TreeNodeValue> tree_nodes = new ArrayList<TreeNodeValue>();
	private Map<Integer, Object> plans = new HashMap<Integer, Object>();
	private SortedMap<Long, TreeNodeValue.Builder> out_tree_nodes = new TreeMap<Long, TreeNodeValue.Builder>();
	private Map<Integer, HoloInfo> builders = new HashMap<Integer, HoloInfo>();

	private class HoloInfo {
		public HoloTreeBuilder builder = null;
		public HoloConfig config = null;
		// 存放每个配置文件中效果页的id，用于建树优化
		public Set<Integer> effectPageIds = null;
	}

	/**
	 * 构造函数
	 * 
	 * @param builder
	 */
	EffectTreeNodeList() {

	}

	EffectTreeNodeList(boolean isAll, boolean root_is_lp) {
		this.isAll = isAll;
		this.root_is_lp = root_is_lp;
	}

	/**
	 * 传hadoop的输出相关类
	 * 
	 * @param output
	 * @param reporter
	 */
	public void setOutputParam(OutputCollector<Text, Text> output,
			Reporter reporter) {
		this.output = output;
		this.reporter = reporter;
	}

	/**
	 * 加载不同配置的holoTreeBuilder
	 * 
	 * @param builder
	 */
	public void addHoloConfig(HoloConfig config) {
		HoloInfo info = new HoloInfo();
		info.builder = new HoloTreeBuilder(config);
		info.config = config;
		RuleSet ruleSet = new RuleSet(config);
		info.effectPageIds = ruleSet.getEffectPageSet();
		builders.put(config.plan_id, info);

	}

	/**
	 * 清空数据
	 */
	public void clear() {
		tree_nodes.clear();
		plans.clear();
		out_tree_nodes.clear();
	}

	/**
	 * 添加树节点到List中，添加过程中判断哪些plan需要保留后续进行建树操作 对于有具体效果页的配置，只对匹配有效果页的session建树
	 * 对于效果页是0的配置，对url matcher过的session建树
	 * 
	 * @param node
	 */
	public void append(TreeNodeValue node) {
		tree_nodes.add(node);

		// 检查是否有match过的plan
		for (TreeNodeValue.TypeRef typeRef : node.getTypeRefList()) {
			if (isAll) {
				plans.put(typeRef.getPlanId(), null);
			} else {
				if (typeRef.getIsMatched()) {
					int planId = typeRef.getPlanId();
					Set<Integer> epIds = builders.get(planId).effectPageIds;
					if (!epIds.contains(0)) {
						if (epIds.contains(typeRef.getPtype())) {
							plans.put(typeRef.getPlanId(), null);
						}
					} else
						plans.put(typeRef.getPlanId(), null);
				}
			}
		}

	}

	/**
	 * 创建树, 并输出到output
	 * 
	 * @param output
	 * @throws IOException
	 */
	public void buildAllTrees() throws IOException {
		for (Integer plan_id : plans.keySet()) {
			buildTree(plan_id);
		}

		/*
		 * // 全部创建完成后统一输出结果 String key = ""; int i = 0; for(Integer plan_id :
		 * builders.keySet()){ if (i==0){key=String.valueOf(plan_id);} else {key
		 * += "-"+String.valueOf(plan_id);} i++; } for (TreeNodeValue.Builder
		 * outputNode : out_tree_nodes.values()) { String data =
		 * LzEffectProtoUtil.toString(outputNode.build()); output.collect(new
		 * Text(key), new Text(data)); }
		 */
	}

	/**
	 * 创建指定plan_id的树, 并输出到output
	 * 
	 * @param plan_id
	 * @param output
	 * @throws IOException
	 */
	private void buildTree(Integer plan_id) throws IOException {
		for (TreeNodeValue node : tree_nodes) {
			PTLogEntry logEntry = genLogEntry(node, plan_id);
			builders.get(plan_id).builder.appendLog(logEntry);
		}

		outputHoloTree(builders.get(plan_id));

		// 每个plan单次输出自己的结果
		for (TreeNodeValue.Builder outputNode : out_tree_nodes.values()) {
			String data = LzEffectProtoUtil.toString(outputNode.build());
			output.collect(new Text(String.valueOf(plan_id)), new Text(data));
			reporter.incrCounter(
					EffectOwnershipStatusCounter.BuilderTreeStatus.UAFG_REDUCE_OUTPUT_RIGHT,
					1);
		}
		out_tree_nodes.clear();

		// 清空建树器，初始化计数器，开始另一轮建树
		builders.get(plan_id).builder.flush();
	}

	/**
	 * 建立树的节点信息
	 * 
	 * @param node
	 * @param plan_id
	 */
	private PTLogEntry genLogEntry(TreeNodeValue node, Integer plan_id) {
		PTLogEntry logEntry = new PTLogEntry();
		List<String> token = new ArrayList<String>();

		// 填充原始日志相关字段信息
		logEntry.put("ts", node.getTs());

		logEntry.put("url", node.getUrl());
		logEntry.put("refer_url", node.getRefer());
		logEntry.put("shop_id", node.getShopId());
		logEntry.put("auction_id", node.getAuctionId());
		logEntry.put("uid", node.getUserId());
		logEntry.put("ali_corp", node.getAliCorp());

		logEntry.put("mid", node.getCookie());
		logEntry.put("sid", node.getSession());
		logEntry.put("mid_uid", node.getCookie2());

		List<TreeNodeValue.KeyValueS> access_useful_extras = node
				.getAccessUsefulExtraList();
		for (TreeNodeValue.KeyValueS userful_extra : access_useful_extras) {
			logEntry.put(userful_extra.getKey(), userful_extra.getValue());
			token.add(userful_extra.getKey() + Constants.CTRL_C
					+ userful_extra.getValue());
		}
		logEntry.put("access_useful_extras",
				StringUtil.join(token, Constants.CTRL_B));
		logEntry.put("access_extra", node.getAccessExtra());

		// 找到需要的plan_id生成logEntry
		for (TreeNodeValue.TypeRef typeRef : node.getTypeRefList()) {
			if (typeRef.getPlanId() == plan_id) {
				logEntry.put("analyzer_id", typeRef.getAnalyzerId());
				logEntry.put("plan_id", typeRef.getPlanId());
				logEntry.setRType(typeRef.getRtype());
				logEntry.setPType(typeRef.getPtype());
				logEntry.setMatched(typeRef.getIsMatched());
				List<TreeNodeValue.KeyValueS> captureInfoList = typeRef
						.getCapturedInfoList();
				for (TreeNodeValue.KeyValueS captureInfo : captureInfoList) {
					logEntry.put(captureInfo.getKey(), captureInfo.getValue());
				}
				List<TreeNodeValue.KeyValueI> sourceInfoList = typeRef
						.getSourceInfoList();
				for (TreeNodeValue.KeyValueI sourceInfo : sourceInfoList) {
					logEntry.getSourceType().put(sourceInfo.getKey(),
							sourceInfo.getValue());
				}
			}
		}
		// 填充完整建树结点
		return logEntry;
	}

	/**
	 * 生成输出树结点的对象列表
	 * 
	 * @param treeBuilder
	 */
	public void outputHoloTree(HoloInfo holoInfo) {
		// 循环遍历树并输出每棵树的结点
		for (SortedMap<Long, HoloTreeNode> holoTree : holoInfo.builder
				.getCurrentTrees()) {
			Iterator<HoloTreeNode> it = holoTree.values().iterator();
			while (it.hasNext()) {
				// 轮询树中的每个树结点
				HoloTreeNode node = it.next();
				TreeNodeValue.Builder outputNode = null;
				TreeNodeValue.TypeRef.Builder typeRefBuilder = null;

				// 判断是否只输出染色结点
				if (!node.getSources().isEmpty()) {
					outputNode = EffectHoloNodeToProto.genBuilder(node);
					typeRefBuilder = EffectHoloNodeToProto
							.genColorTypeRefBuilder(node,
									holoInfo.config.lookahead);
				} else if (isAll) {
					if (root_is_lp) {
						// 做b2c来源特制
						outputNode = EffectHoloNodeToProto.genBuilder(node);
						typeRefBuilder = EffectHoloNodeToProto
								.genRootIsLPTypeRefBuilder(node,
										holoInfo.config.lookahead);
					} else {
						outputNode = EffectHoloNodeToProto.genBuilder(node);
						typeRefBuilder = EffectHoloNodeToProto
								.genNormalTypeRefBuilder(node);
					}
				}

				if (outputNode != null) {
					if (!out_tree_nodes.containsKey(node.getUniqTS())) {
						out_tree_nodes.put(node.getUniqTS(), outputNode);
						reporter.incrCounter(
								EffectOwnershipStatusCounter.BuilderTreeStatus.UAFG_HOLOTREE_OUTPUT_NODE_COUNT,
								1);
					} else {
						System.err.println(String.valueOf(node.getUniqTS()));
						outputNode.getSession();
					}
					addTypeRef(node.getUniqTS(), typeRefBuilder.build());
					reporter.incrCounter(
							EffectOwnershipStatusCounter.BuilderTreeStatus.UAFG_HOLOTREE_OUTPUT_RIGHT,
							1);
				} else {
					reporter.incrCounter(
							EffectOwnershipStatusCounter.BuilderTreeStatus.UAFG_HOLOTREE_OUTPUT_ERROR,
							1);
				}
			}
		}
	}

	/**
	 * 
	 * @param uniqKey
	 * @param node
	 */
	private void addTypeRef(Long uniqKey, TreeNodeValue.TypeRef typeRef) {
		out_tree_nodes.get(uniqKey).addTypeRef(typeRef);
	}
}
