package com.etao.data.ep.ownership;

import java.util.ArrayList;
import java.util.List;

import com.etao.data.ep.ownership.proto.LzEffectProto.TreeNodeValue;
import com.etao.lz.dw.util.Constants;

public class EffectOwnershipNode {
	// 节点属性
	String index_root_path = "";
	long ts = 0;
	String url = "";
	String refer = "";
	String cookie = "";
	String session = "";
	String cookie2 = "";
	String shop_id = "";
	String auction_id = "";
	String user_id = "";
	int ali_corp = 0;
	boolean is_leaf = false;
	
	String access_useful_extra = "";
	String access_extra = "";
	
	// 特殊plan_id节点属性
	int analyzer_id;
	int plan_id;
	String attr_calc = "";
	
	List<EffectOwnershipPathinfo> path_infos = new ArrayList<EffectOwnershipPathinfo>();
	
	public EffectOwnershipNode(){
		
	}
	
	public EffectOwnershipNode(TreeNodeValue value){
		
	}
	
	
	public List<EffectOwnershipPathinfo> getPath_infos() {
		return path_infos;
	}

	public List<String> toStringList() {
		List<String> result = new ArrayList<String>();
		
		for(EffectOwnershipPathinfo path_info : path_infos) {
			StringBuffer sb = new StringBuffer();
			sb.append(index_root_path).append(Constants.CTRL_A);
			sb.append(ts).append(Constants.CTRL_A);
			sb.append(analyzer_id).append(Constants.CTRL_A);
			sb.append(plan_id).append(Constants.CTRL_A);
			sb.append(path_info.src).append(Constants.CTRL_A);
			sb.append(url).append(Constants.CTRL_A);
			sb.append(refer).append(Constants.CTRL_A);
			sb.append(shop_id).append(Constants.CTRL_A);
			sb.append(auction_id).append(Constants.CTRL_A);
			sb.append(user_id).append(Constants.CTRL_A);
			sb.append(ali_corp).append(Constants.CTRL_A);
			sb.append(cookie).append(Constants.CTRL_A);
			sb.append(session).append(Constants.CTRL_A);
			sb.append(cookie2).append(Constants.CTRL_A);
			sb.append(path_info.is_effect_page?1:0).append(Constants.CTRL_A);
			sb.append(path_info.ref_is_effect_page?1:0).append(Constants.CTRL_A);
			sb.append(is_leaf?1:0).append(Constants.CTRL_A);
			sb.append(path_info.jump_num).append(Constants.CTRL_A);
			sb.append(path_info.index_type).append(Constants.CTRL_A);
			sb.append(path_info.pv).append(Constants.CTRL_A);
			sb.append(path_info.gmv_amt).append(Constants.CTRL_A);
			sb.append(path_info.gmv_auction_num).append(Constants.CTRL_A);
			sb.append(path_info.gmv_trade_num).append(Constants.CTRL_A);
			sb.append(path_info.alipay_amt).append(Constants.CTRL_A);
			sb.append(path_info.alipay_auction_num).append(Constants.CTRL_A);
			sb.append(path_info.alipay_trade_num).append(Constants.CTRL_A);
			sb.append(path_info.item_collect_num).append(Constants.CTRL_A);
			sb.append(path_info.shop_collect_num).append(Constants.CTRL_A);
			sb.append(path_info.cart_auction_num).append(Constants.CTRL_A);
			sb.append(this.access_useful_extra).append(Constants.CTRL_A);
			sb.append(this.access_extra);

			result.add( sb.toString() );
		}
		
		if( result.isEmpty() ) {
			System.err.println("此节点无法输出，因为没有path信息！");
		}
		return result;
	}
	
}
