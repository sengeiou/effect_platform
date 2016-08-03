package com.ali.lz.effect.ownership;

import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;

public class EffectOwnershipGmvNode extends EffectOwnershipActionNode {

    private float gmv_amt = 0;
    private float gmv_auction_num = 0;
    private float gmv_trade_num = 0;
    private float alipay_amt = 0;
    private float alipay_auction_num = 0;
    private float alipay_trade_num = 0;

    public EffectOwnershipGmvNode(TreeNodeValue value) {
        super(value);

        gmv_amt = value.getGmvAmt();
        gmv_auction_num = value.getGmvAuctionNum();
        gmv_trade_num = value.getGmvTradeNum();
        alipay_amt = value.getAlipayAmt();
        alipay_auction_num = value.getAlipayAuctionNum();
        alipay_trade_num = value.getAlipayTradeNum();
        access_extra = value.getAccessExtra();

    }

    @Override
    public void calcEffects() {
        for (EffectOwnershipPathinfo path_info : path_infos) {
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
