package com.ali.lz.effect.ownership;

import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue.TypeRef.TypePathInfo;

/**
 * 用来标识规则配置中一条来源路径的各种属性
 * 对于同一条来源路径，在染色过程中记录了该路径在单个用户访问树中首次出现和末次出现的信息。当前默认取末次出现的信息。
 */
public class EffectOwnershipPathinfo {
    String src = "";
    long first_ts;
    long last_ts;
    int priority;
    boolean is_effect_page;
    boolean ref_is_effect_page;
    int first_guide_jump_num;
    String first_guide_auction_id = "";
    String first_guide_shop_id = "";
    int last_guide_jump_num;
    String last_guide_auction_id = "";
    String last_guide_shop_id = "";

    // 效果属性信息
    int jump_num;
    int index_type; // 指标类型标识(0非店铺, 1单品, 2单品同店, 3单店, 4淘外成交(仅etao才有), 5单品其他,
    // 6单店其他)

    float pv = 0;
    float gmv_amt = 0;
    float gmv_auction_num = 0;
    float gmv_trade_num = 0;
    float alipay_amt = 0;
    float alipay_auction_num = 0;
    float alipay_trade_num = 0;
    float item_collect_num = 0;
    float shop_collect_num = 0;
    float cart_auction_num = 0;

    EffectOwnershipPathinfo() {

    }

    EffectOwnershipPathinfo(EffectOwnershipPathinfo info) {
        // 自身clone时不携带效果页属性
        src = info.src;
        first_ts = info.first_ts;
        last_ts = info.last_ts;
        priority = info.priority;
        is_effect_page = false;
        ref_is_effect_page = false;
        first_guide_jump_num = info.first_guide_jump_num;
        first_guide_auction_id = info.first_guide_auction_id;
        first_guide_shop_id = info.first_guide_shop_id;
        last_guide_jump_num = info.last_guide_jump_num;
        last_guide_auction_id = info.last_guide_auction_id;
        last_guide_shop_id = info.last_guide_shop_id;
    }

    EffectOwnershipPathinfo(TypePathInfo path_info) {
        src = path_info.getSrc();
        first_ts = path_info.getFirstTs();
        last_ts = path_info.getLastTs();
        priority = path_info.getPriority();
        is_effect_page = path_info.getIsEffectPage();
        ref_is_effect_page = path_info.getRefIsEffectPage();
        first_guide_jump_num = path_info.getFirstGuideJumpNum();
        first_guide_auction_id = path_info.getFirstGuideAuctionId();
        first_guide_shop_id = path_info.getFirstGuideShopId();
        last_guide_jump_num = path_info.getLastGuideJumpNum();
        last_guide_auction_id = path_info.getLastGuideAuctionId();
        last_guide_shop_id = path_info.getLastGuideShopId();
    }

    public int getPriority() {
        return priority;
    }

    public long getFirstTs() {
        return first_ts;
    }

    public long getLastTs() {
        return last_ts;
    }

    /**
     * 根据节点auction_id和shop_id计算所属指标类型。 指标类型为：0非店铺, 1单品, 2单品同店, 3单店,
     * 4淘外成交(仅etao才有), 5单品其他, 6单店其他 在同一个path中，全部按照最近归属来计算
     * 
     * @param auction_id
     * @param shop_id
     * @param attr_calc
     *            暂时不使用
     * @return
     */
    // TODO: 梳理attr_calc在染色过程中的作用
    public int calcIndexProperty(String auction_id, String shop_id, String attr_calc) {
        /*
         * first - 归属至从源头开始首个来源(即离效果发生处最远的来源) last -
         * 归属至从源头开始最后一个来源(即离效果发生处最近的来源) equal - 所有踩中的来源均分效果 all -
         * 所有踩中的来源同时得到相同效果
         */
        // if (attr_calc.equals("all")) {
        // // 这里计算有问题？？无法归属，先按照最近
        // if (auction_id.length() > 0
        // && auction_id.equals(last_guide_auction_id)) {
        // return 1;
        // } else if (shop_id.length() > 0
        // && shop_id.equals(last_guide_shop_id)) {
        // return 2;
        // } else if (shop_id.length() > 0
        // && !shop_id.equals(last_guide_shop_id)) {
        // return 3;
        // } else {
        // return 0;
        // }
        // } else if (attr_calc.equals("equal")) {
        // // 本期不做
        // return 0;
        // } else if (attr_calc.equals("first")) {
        // if (auction_id.length() > 0
        // && auction_id.equals(first_guide_auction_id)) {
        // return 1;
        // } else if (shop_id.length() > 0
        // && shop_id.equals(first_guide_shop_id)) {
        // return 2;
        // } else if (shop_id.length() > 0
        // && !shop_id.equals(first_guide_shop_id)) {
        // return 3;
        // } else {
        // return 0;
        // }
        // } else { // 默认last*/
        jump_num = last_guide_jump_num;
        if (last_guide_auction_id.length() > 0 && auction_id.length() > 0) {
            if (auction_id.equals(last_guide_auction_id)) {
                return 1; // 单品
            } else if (shop_id.length() > 0 && shop_id.equals(last_guide_shop_id)) {
                return 2; // 单品同店
            } else {
                return 5; // 单品其他
            }
        } else if (last_guide_shop_id.length() > 0 && shop_id.length() > 0) {
            if (shop_id.equals(last_guide_shop_id)) {
                return 3; // 单店
            } else {
                return 6; // 单店其他
            }
        } else {
            return 0; // 非店铺
        }
    }

    public int getIndex_type() {
        return index_type;
    }

    public void setIndex_type(int index_type) {
        this.index_type = index_type;
    }

    public void initGmvIndex() {
        gmv_amt = 0;
        gmv_auction_num = 0;
        gmv_trade_num = 0;
        alipay_amt = 0;
        alipay_auction_num = 0;
        alipay_trade_num = 0;
    }

    public void initPvIndex() {
        pv = 0;
    }

    public void initCollectIndex() {
        item_collect_num = 0;
        shop_collect_num = 0;
    }

    public void initCartIndex() {
        cart_auction_num = 0;
    }
}
