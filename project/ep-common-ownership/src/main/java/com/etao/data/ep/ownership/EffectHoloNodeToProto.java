package com.etao.data.ep.ownership;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Map;

import com.etao.lz.effect.HoloTreeNode;
import com.etao.lz.effect.SourceMeta;
import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;
import com.etao.lz.dw.util.Constants;

public class EffectHoloNodeToProto {
	/**
	 * 创建builder, 返回node中公共内容
	 * @param node
	 * @return 返回builder
	 */
	public static TreeNodeValue.Builder genBuilder(HoloTreeNode node) {
		TreeNodeValue.KeyValueS.Builder keyValueS_builder = TreeNodeValue.KeyValueS
				.newBuilder();
		TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

		builder.setTs((Long) node.getPtLogEntry().get("ts"));

		builder.setIndexRootPath(node.getSerialRootPath());
		builder.setIsLeaf(node.getChildren().size() > 0 ? false : true);
		builder.setIsRoot(node.getParent() != null ? false : true);

		builder.setUrl(((String) node.getPtLogEntry().get("url"))
				.split(Constants.CTRL_A)[0]);
		builder.setRefer(((String) node.getPtLogEntry().get("refer_url"))
				.split(Constants.CTRL_A)[0]);
		builder.setShopId((String) node.getPtLogEntry().get("shop_id"));
		builder.setAuctionId((String) node.getPtLogEntry().get("auction_id"));
		builder.setUserId((String) node.getPtLogEntry().get("uid"));
		builder.setAliCorp((Integer) node.getPtLogEntry().get("ali_corp"));

		builder.setCookie((String) node.getPtLogEntry().get("mid"));
		builder.setSession((String) node.getPtLogEntry().get("sid"));
		builder.setCookie2((String) node.getPtLogEntry().get("mid_uid"));

		String[] useful_extras = ((String)node.getPtLogEntry().get("access_useful_extras")).split(Constants.CTRL_B, -1);
		for (String useful_extra : useful_extras){
			String[] keyValue = useful_extra.split(Constants.CTRL_C, -1);
			if(keyValue.length != 2){
				continue;
			}
			keyValueS_builder.setKey(keyValue[0]);
			keyValueS_builder.setValue(keyValue[1]);
			builder.addAccessUsefulExtra(keyValueS_builder);
		}
		builder.setAccessExtra((String)node.getPtLogEntry().get("access_extra"));

		return builder;
	}

	/**
	 * 生成普通树结点的probuf里typeref的 builder
	 * @param node
	 * @return
	 */
	public static TreeNodeValue.TypeRef.Builder genNormalTypeRefBuilder(HoloTreeNode node) {
		TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef
				.newBuilder();

		type_builder.setAnalyzerId((Integer) node.getPtLogEntry().get(
				"analyzer_id"));
		type_builder.setPlanId((Integer) node.getPtLogEntry().get("plan_id"));
		type_builder.setIsMatched(node.getPtLogEntry().matched());
		type_builder.setRtype(node.getPtLogEntry().getRType());
		type_builder.setPtype(node.getPtLogEntry().getPType());

		return type_builder;
	}

	/**
	 * 生成染色树结点的probuf里typeref的 builder
	 * @param node
	 * @return
	 */
	public static TreeNodeValue.TypeRef.Builder genColorTypeRefBuilder(HoloTreeNode node, int[] lookahead) {
		TreeNodeValue.TypeRef.TypePathInfo.Builder tpinfo_builder = TreeNodeValue.TypeRef.TypePathInfo
				.newBuilder();
		TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef
				.newBuilder();

		type_builder = genNormalTypeRefBuilder(node);

		int max_priority=Integer.MAX_VALUE;
		Map<String, SourceMeta> nodeSrcs = node.getSources();
		for (Map.Entry<String, SourceMeta> src : nodeSrcs.entrySet()) {
			// 对返回的结果的优先级进行处理，只保留最高优先级的结果
			if(src.getValue().getPriority() > max_priority){
				continue;
			} else {
				type_builder.clearPathInfo();
				max_priority = src.getValue().getPriority();
			}
			
			tpinfo_builder.setSrc(src.getKey());
			tpinfo_builder.setFirstTs(src.getValue().getFirstOpTS());
			tpinfo_builder.setLastTs(src.getValue().getLastOpTS());
			tpinfo_builder.setPriority(src.getValue().getPriority());
			tpinfo_builder.setIsEffectPage(node.isEffectPage());
			if (node.getParent() != null) {
				tpinfo_builder.setRefIsEffectPage(node.getParent()
						.isEffectPage());
			} else
				tpinfo_builder.setRefIsEffectPage(false);

			HoloTreeNode firstEP = src.getValue().getFirstEP();
			HoloTreeNode lastEP = src.getValue().getLastEP();
			String[] firstASid, lastASid;
			if (firstEP == lastEP) {
				firstASid = lastASid = getGuideId(firstEP, node, lookahead);
			} else {
				firstASid = getGuideId(firstEP, node, lookahead);
				lastASid = getGuideId(lastEP, node, lookahead);
			}
			tpinfo_builder.setFirstGuideJumpNum(Integer.parseInt(firstASid[0]));
			tpinfo_builder.setFirstGuideAuctionId(firstASid[1]);
			tpinfo_builder.setFirstGuideShopId(firstASid[2]);
			tpinfo_builder.setLastGuideJumpNum(Integer.parseInt(lastASid[0]));
			tpinfo_builder.setLastGuideAuctionId(lastASid[1]);
			tpinfo_builder.setLastGuideShopId(lastASid[2]);

			type_builder.addPathInfo(tpinfo_builder);
		}

		return type_builder;
	}

	/**
	 * 针对B2C这种特殊情况设计。就是所有的如果根节点都必须归属给一个来源，
	 * 所有Root当不是matched的情况，需要给一个默认值作为来源。默认给src=-1
	 * 逻辑：所有的没有被染色的节点，都一直向上追溯到根节点，然后Ts, EP都要给root节点的数值。
	 * @param node
	 * @param lookahead
	 * @return
	 */
	public static TreeNodeValue.TypeRef.Builder genRootIsLPTypeRefBuilder(HoloTreeNode node, int[] lookahead) {
		TreeNodeValue.TypeRef.TypePathInfo.Builder tpinfo_builder = TreeNodeValue.TypeRef.TypePathInfo
				.newBuilder();
		TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef
				.newBuilder();

		type_builder = genNormalTypeRefBuilder(node);

		if (!node.getSources().isEmpty()) {
			int max_priority=Integer.MAX_VALUE;
			Map<String, SourceMeta> nodeSrcs = node.getSources();
			for (Map.Entry<String, SourceMeta> src : nodeSrcs.entrySet()) {
				// 对返回的结果的优先级进行处理，只保留最高优先级的结果
				if(src.getValue().getPriority() > max_priority){
					continue;
				} else {
					type_builder.clearPathInfo();
					max_priority = src.getValue().getPriority();
				}
			tpinfo_builder.setSrc(src.getKey());
			tpinfo_builder.setPriority(src.getValue().getPriority());
			}
		}else{
			// 此处全部给默认值
			tpinfo_builder.setSrc("-1\002");
			tpinfo_builder.setPriority(Integer.MAX_VALUE);
		}

		// 此处查找到root_node，然后计算其他指标。
		HoloTreeNode root_node = node;
		while (root_node.getParent() != null) {
			root_node = root_node.getParent();
			continue;
		}

		if (root_node == node) {
			tpinfo_builder.setIsEffectPage(true);
			node.setEffectPage(true);
		} else if (node.getParent() == root_node) {
			tpinfo_builder.setRefIsEffectPage(true);
		}

		tpinfo_builder.setFirstTs((Long) root_node.getPtLogEntry().get("ts"));
		tpinfo_builder.setLastTs((Long) root_node.getPtLogEntry().get("ts"));

		String[] firstASid, lastASid;
		firstASid = lastASid = getGuideId(root_node, node, lookahead);

		tpinfo_builder.setFirstGuideJumpNum(Integer.parseInt(firstASid[0]));
		tpinfo_builder.setFirstGuideAuctionId(firstASid[1]);
		tpinfo_builder.setFirstGuideShopId(firstASid[2]);
		tpinfo_builder.setLastGuideJumpNum(Integer.parseInt(lastASid[0]));
		tpinfo_builder.setLastGuideAuctionId(lastASid[1]);
		tpinfo_builder.setLastGuideShopId(lastASid[2]);

		type_builder.addPathInfo(tpinfo_builder);

		return type_builder;
	}
	/**
	 * 生成输出树结点的probuf builder
	 * 
	 * @param node
	 *            HoloTreeNode
	 * @return TreeNodeValue.Builder
	 */
	public static TreeNodeValue.Builder genNormalBuilder(HoloTreeNode node) {
		TreeNodeValue.Builder builder = genBuilder(node);

		TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef
				.newBuilder();
		type_builder = genNormalTypeRefBuilder(node);
		builder.addTypeRef(type_builder);

		return builder;
	}

	public static TreeNodeValue.Builder genColorBuilder(HoloTreeNode node, int[] lookahead) {
		TreeNodeValue.Builder builder = genBuilder(node);
		builder.addTypeRef(genColorTypeRefBuilder(node, lookahead));

		return builder;
	}


	/**
	 * 得到当前节点对应的引导宝贝/店铺
	 * 首先向根节点回溯，获取效果页节点及效果页后的指定跳数节点
	 * 然后从效果页节点开始寻找引导宝贝页，在lookahead指定的最大跳数内找到首个宝贝页或店铺页即停止，如果未找到则留空
	 * @param holoTreeNode
	 * @param node
	 * @param lookahead 配置文件中用户指定的中间页条数
	 * @return jump_num 如果在指定跳数内找到引导宝贝或店铺页，则返回从效果页至引导宝贝/店铺页的跳数；如果未找到，则返回-1
	 * @return guideAuctionId, guideShopId 如果未找到，返回""
	 */
	private static String[] getGuideId(HoloTreeNode holoTreeNode, HoloTreeNode node, int[] lookahead) {
		String[] auctionShopId = new String[3];
		String guideAuctionId = "";
		String guideShopId = "";
		int jump_num = 0;

		if(lookahead == null || lookahead.length == 0)
			lookahead = new int[]{1};
		int lookaheadLength = lookahead.length;
		int lookaheadMax = lookahead[lookaheadLength-1];
		ArrayDeque<HoloTreeNode> trackNodeDeque = new ArrayDeque<HoloTreeNode>(lookaheadMax);

		HoloTreeNode effectNode = node;
		// 向根节点回溯，获取效果页节点及效果页后的指定跳数节点
		while (!effectNode.isEffectPage()) {
			trackNodeDeque.addFirst(effectNode);
                        if(effectNode.getParent() != null){
			    effectNode = effectNode.getParent();
                        }else{
                            break; 
                        }
		}

		// 从效果页节点开始寻找引导宝贝页，在lookahead指定的最大跳数内找到首个宝贝页或店铺页，且jump_num在lookahead跳数列表内即停止
		guideAuctionId = (String) effectNode.getPtLogEntry().get(
				"auction_id");
		guideShopId = (String) effectNode.getPtLogEntry().get("shop_id");
		if (guideShopId.length() <= 0 && guideAuctionId.length() <= 0) {
			HoloTreeNode trackNode = null;
			while ((trackNode = trackNodeDeque.pollFirst()) != null && jump_num < lookaheadMax) {
				jump_num++;
				if (Arrays.binarySearch(lookahead, jump_num) >= 0) {
					guideAuctionId = (String) trackNode.getPtLogEntry().get("auction_id");
					guideShopId = (String) trackNode.getPtLogEntry().get("shop_id");
					if (guideAuctionId.length() > 0 || guideShopId.length() > 0)
						break;
				}
			}
			if (guideShopId.length() <= 0 && guideAuctionId.length() <= 0) {
				// 遍历后仍未找到，jump_num为-1
				jump_num = -1;
			}
		}

		auctionShopId[0] = String.valueOf(jump_num);
		auctionShopId[1] = guideAuctionId;
		auctionShopId[2] = guideShopId;
		return auctionShopId;

	}
}
