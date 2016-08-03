package com.etao.data.ep.ownership;

import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;

public class EffectOwnershipGmvTreeNode extends EffectOwnershipNode{

	private float gmv_amt = 0;
	private float gmv_auction_num = 0;
	private float gmv_trade_num = 0;
	private float alipay_amt = 0;
	private float alipay_auction_num = 0;
	private float alipay_trade_num = 0;

	
	public EffectOwnershipGmvTreeNode(TreeNodeValue value) {
		super(value);
		
		ts = value.getTs();
		shop_id = value.getShopId();
		auction_id = value.getAuctionId();
		user_id = value.getUserId();
		ali_corp = value.getAliCorp();
		
		gmv_amt = value.getGmvAmt();
		gmv_auction_num = value.getGmvAuctionNum();
		gmv_trade_num = value.getGmvTradeNum();
		alipay_amt = value.getAlipayAmt();
		alipay_auction_num = value.getAlipayAuctionNum();
		alipay_trade_num = value.getAlipayTradeNum();
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
		
		access_useful_extra = node.access_useful_extra;
		access_extra = node.access_extra;
		
		node.clonePathInfo(path_infos);
	}
	
	public void calcGmvInfo() {
		for(EffectOwnershipPathinfo path_info:path_infos) {
			int index_prop = path_info.calcIndexProperty(auction_id, shop_id, attr_calc);
			path_info.initGmvIndex();

			path_info.index_type = index_prop;
			path_info.gmv_amt = gmv_amt;
			path_info.gmv_auction_num = gmv_auction_num;
			path_info.gmv_trade_num = gmv_trade_num;
			path_info.alipay_amt = alipay_amt;
			path_info.alipay_auction_num = alipay_auction_num;
			path_info.alipay_trade_num = alipay_trade_num;
		}
	}
}
