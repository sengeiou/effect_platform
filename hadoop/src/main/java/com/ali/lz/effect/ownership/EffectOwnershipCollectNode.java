package com.ali.lz.effect.ownership;

import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;

public class EffectOwnershipCollectNode extends EffectOwnershipActionNode {

    private float shop_collect_num = 0;
    private float item_collect_num = 0;

    public EffectOwnershipCollectNode(TreeNodeValue value) {
        super(value);

        shop_collect_num = value.getShopCollectNum();
        item_collect_num = value.getItemCollectNum();
    }

    @Override
    public void calcEffects() {
        for (EffectOwnershipPathinfo path_info : path_infos) {
            int index_prop = path_info.calcIndexProperty(auction_id, shop_id, attr_calc);
            path_info.initCollectIndex();

            path_info.index_type = index_prop;
            path_info.shop_collect_num = this.shop_collect_num;
            path_info.item_collect_num = this.item_collect_num;
        }
    }
}
