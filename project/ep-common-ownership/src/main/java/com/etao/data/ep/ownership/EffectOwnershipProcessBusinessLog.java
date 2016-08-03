package com.etao.data.ep.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

/*
 * 用来保存符合用户归属条件的浏览日志
 */
public class EffectOwnershipProcessBusinessLog {

	public OutputCollector<Text, Text> output;
	List<EffectOwnershipAccessTreeNode> nodes = new ArrayList<EffectOwnershipAccessTreeNode>();

	// 用于不需要输出访问日志节点的效果归属，比如站外成交归属
	public EffectOwnershipProcessBusinessLog() {
		this.output = null;
	}

	public EffectOwnershipProcessBusinessLog(OutputCollector<Text, Text> output) {
		this.output = output;
	}

	public void clearNodes() throws IOException {
		// 先输出，再清理
		for (EffectOwnershipAccessTreeNode node : nodes) {
			this.output(node);
		}
		nodes.clear();
	}

	/*
	 * append浏览日志，保存符合条件的。 如果新节点信息符合条件，则放入保存队列，把替换的节点output。
	 * 如果新节点信息不符合条件，则直接output输出。
	 */
	public void appendAccessLog(EffectOwnershipAccessTreeNode node,
			String attr_calc) throws IOException {
		/*
		 * (a) 来源路径对应规则优先级不同时, 归属至规则优先级高的来源路径; (b) 来源路径对应规则优先级相同时,
		 * 按照用户选择的同优先级归属方法进行归属; (c) 同优先级归属方法为 first/last 时, 需要根据各来源路径的归属点出现时刻先后
		 * 顺序决定归属给谁
		 */
		boolean bAppend = false;
		if (nodes.isEmpty()) {
			bAppend = true;
			nodes.add(node);
		} else if (node.GetPlanPathPriority(0) < nodes.get(0)
				.GetPlanPathPriority(0)) {
			clearNodes();
			bAppend = true;
			nodes.add(node);
		} else if (node.GetPlanPathPriority(0) == nodes.get(0)
				.GetPlanPathPriority(0)) {
			/*
			 * first - 归属至从源头开始首个来源(即离效果发生处最远的来源) last -
			 * 归属至从源头开始最后一个来源(即离效果发生处最近的来源) equal - 所有踩中的来源均分效果 all -
			 * 所有踩中的来源同时得到相同效果
			 */
			if (attr_calc.equals("all")) {
				bAppend = true;
				nodes.add(node);
			} else if (attr_calc.equals("equal")) {
				// 本期不做
				bAppend = true;
				nodes.add(node);

			} else if (attr_calc.equals("first")) {
				if (node.GetPlanPathFirstTs(0) < nodes.get(0)
						.GetPlanPathFirstTs(0)) {
					clearNodes();
					bAppend = true;
					nodes.add(node);
				} else if (node.GetPlanPathFirstTs(0) == nodes.get(0)
						.GetPlanPathFirstTs(0)) {
					// 同路径同优先级，优先取宝贝或店铺引导页归属
					// 同路径同优先级，多个引导页， 取离效果页最近的引导页归属；多个引导页离效果页等距时，取离成交时间最近的引导页
					String guide_auction_id = node.getPath_infos().get(0).last_guide_auction_id;
					String guide_shop_id = node.getPath_infos().get(0).last_guide_shop_id;
					int guide_jump_num = node.getPath_infos().get(0).jump_num;
					if ((guide_auction_id != null && guide_auction_id.length() > 0)
							|| (guide_shop_id != null && guide_shop_id.length() > 0)) {
						int jump_num = nodes.get(0).getPath_infos().get(0).jump_num;
						if (jump_num < 0 || guide_jump_num <= jump_num) {
							clearNodes();
							bAppend = true;
							nodes.add(node);
						}
					}
				}
			} else { // 默认last
				if (node.GetPlanPathLastTs(0) > nodes.get(0).GetPlanPathLastTs(
						0)) {
					clearNodes();
					bAppend = true;
					nodes.add(node);
				} else if (node.GetPlanPathFirstTs(0) == nodes.get(0)
						.GetPlanPathFirstTs(0)) {
					// 同路径同优先级，优先取宝贝或店铺引导页归属
					// 同路径同优先级，多个引导页， 取离效果页最近的引导页归属；多个引导页离效果页等距时，取离成交时间最近的引导页
					String guide_auction_id = node.getPath_infos().get(0).last_guide_auction_id;
					String guide_shop_id = node.getPath_infos().get(0).last_guide_shop_id;
					int guide_jump_num = node.getPath_infos().get(0).jump_num;
					if ((guide_auction_id != null && guide_auction_id.length() > 0)
							|| (guide_shop_id != null && guide_shop_id.length() > 0)) {
						int jump_num = nodes.get(0).getPath_infos().get(0).jump_num;
						if (jump_num < 0 || guide_jump_num <= jump_num) {
							clearNodes();
							bAppend = true;
							nodes.add(node);
						}
					}
				}
			}
		}

		if (!bAppend) {
			this.output(node);
		}
	}

	public List<EffectOwnershipAccessTreeNode> getNodes() {
		return nodes;
	}

	public void output(EffectOwnershipAccessTreeNode node) throws IOException {
		if (this.output != null) {
			for (String result : node.toStringList()) {
				output.collect(new Text(String.valueOf(node.plan_id)),
						new Text(result));
			}
		}
	}
}
