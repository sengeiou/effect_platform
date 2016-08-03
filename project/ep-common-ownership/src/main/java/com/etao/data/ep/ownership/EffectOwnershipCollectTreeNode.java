package com.etao.data.ep.ownership;

import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;

public class EffectOwnershipCollectTreeNode extends EffectOwnershipNode {

	private float shop_collect_num = 0;
	private float item_collect_num = 0;
	
	public EffectOwnershipCollectTreeNode(TreeNodeValue value) {
		super(value);
		
		ts = value.getTs();
		shop_id = value.getShopId();
		auction_id = value.getAuctionId();
		user_id = value.getUserId();
		ali_corp = value.getAliCorp();
		
		shop_collect_num = value.getShopCollectNum();
		item_collect_num = value.getItemCollectNum();
	}
	
	public void CopyFromAccessTreeNode(EffectOwnershipAccessTreeNode node){
		ts = node.ts;
		analyzer_id = node.analyzer_id;
		plan_id = node.plan_id;
		index_root_path = node.index_root_path;
		attr_calc = node.attr_calc;
		cookie = node.cookie;
		session = node.session;
		cookie2 = node.cookie2;
		
		node.clonePathInfo(path_infos);
	}
	
	public void calcCollectInfo() {
		for(EffectOwnershipPathinfo path_info:path_infos) {
			int index_prop = path_info.calcIndexProperty(auction_id, shop_id, attr_calc);
			path_info.initCollectIndex();

			path_info.index_type = index_prop;
			path_info.shop_collect_num = this.shop_collect_num;
			path_info.item_collect_num = this.item_collect_num;
		}
	}
}
