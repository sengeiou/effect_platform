package com.ali.lz.effect.ownership;

import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.utils.Constants;

public class EffectOwnershipActionNode extends EffectOwnershipNode {

    public EffectOwnershipActionNode(TreeNodeValue value) {
        super(value);
        ts = value.getTs();
        shop_id = value.getShopId();
        auction_id = value.getAuctionId();
        user_id = value.getUserId();
        ali_corp = value.getAliCorp();
    }

    public void CopyFromHoloTreeNode(EffectOwnershipHoloTreeNode node) {
        ts = node.ts;
        analyzer_id = node.analyzer_id;
        plan_id = node.plan_id;
        index_root_path = node.index_root_path;
        attr_calc = node.attr_calc;
        cookie = node.cookie;
        session = node.session;
        visit_id = node.visit_id;

        access_useful_extra = node.access_useful_extra;
        if (access_extra.length() > 0) {
            access_extra += Constants.CTRL_B + node.access_extra;
        } else {
            access_extra = node.access_extra;
        }
        srcUsefulExtra = node.srcUsefulExtra;

        node.clonePathInfo(path_infos);
    }
    
    public void calcEffects() {
        
    }

}
