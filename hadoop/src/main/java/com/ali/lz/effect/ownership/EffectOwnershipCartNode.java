package com.ali.lz.effect.ownership;

import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;

public class EffectOwnershipCartNode extends EffectOwnershipActionNode {

    private float cart_num = 0;

    public EffectOwnershipCartNode(TreeNodeValue value) {
        super(value);
        cart_num = value.getCartNum();
    }

    @Override
    public void calcEffects() {
        for (EffectOwnershipPathinfo path_info : path_infos) {
            int index_prop = path_info.calcIndexProperty(auction_id, shop_id, attr_calc);
            path_info.initCartIndex();

            path_info.index_type = index_prop;
            path_info.cart_auction_num = this.cart_num;
        }
    }
}
