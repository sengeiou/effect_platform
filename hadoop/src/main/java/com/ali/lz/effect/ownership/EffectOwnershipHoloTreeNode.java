package com.ali.lz.effect.ownership;

import java.util.ArrayList;
import java.util.List;

import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue.TypeRef;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue.TypeRef.TypePathInfo;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;

/*
 * 代表了在一个方案下的一条浏览日志
 */
public class EffectOwnershipHoloTreeNode extends EffectOwnershipNode {

    public EffectOwnershipHoloTreeNode(TreeNodeValue value) {
        super(value);

        index_root_path = value.getIndexRootPath();
        ts = value.getTs();
        url = value.getUrl();
        refer = value.getRefer();
        cookie = value.getCookie();
        session = value.getSession();
        visit_id = value.getVisitId();
        shop_id = value.getShopId();
        auction_id = value.getAuctionId();
        user_id = value.getUserId();
        ali_corp = value.getAliCorp();
        is_leaf = value.getIsLeaf();

        List<String> token = new ArrayList<String>();
        List<TreeNodeValue.KeyValueS> access_useful_extras = value.getAccessUsefulExtraList();
        for (TreeNodeValue.KeyValueS userful_extra : access_useful_extras) {
            token.add(userful_extra.getKey() + Constants.CTRL_C + userful_extra.getValue());
        }
        access_useful_extra = StringUtil.join(token, Constants.CTRL_B);
        access_extra = value.getAccessExtra();

        List<TreeNodeValue.KeyValueS> srcUsefulExtras = value.getSrcUsefulExtraList();
        token.clear();
        for (TreeNodeValue.KeyValueS userfulExtra : srcUsefulExtras) {
            token.add(userfulExtra.getKey() + Constants.CTRL_C + userfulExtra.getValue());
        }
        srcUsefulExtra = StringUtil.join(token, Constants.CTRL_B);
        page_duration = value.getPageDuration();
    }

    public void cleanPlanInfo() {
        // 清理plan相关信息
        analyzer_id = -1;
        plan_id = -1;
        attr_calc = null;
        path_infos.clear();
    }

    public void setPlanInfo(TypeRef type_ref, String attr_calc) {
        // 清理plan相关信息，赋值新的
        analyzer_id = type_ref.getAnalyzerId();
        plan_id = type_ref.getPlanId();
        this.attr_calc = attr_calc;
        path_infos.clear();

    }

    /**
     * 添加path到流量日志，规则：先判定priority, 再判断attr_calc
     * 
     * @param t_path_info
     */
    public void setPlanPathInfo(TypePathInfo t_path_info) {

        EffectOwnershipPathinfo path_info = new EffectOwnershipPathinfo(t_path_info);
        // 判定浏览归属给哪个指标
        int index_prop = path_info.calcIndexProperty(auction_id, shop_id, attr_calc);
        path_info.initPvIndex();
        path_info.index_type = index_prop;
        path_info.pv = 1;

        // 判断保留哪些path_infos来使用
        if (path_infos.size() == 0) {
            path_infos.add(path_info);
        } else if (path_info.getPriority() < path_infos.get(0).getPriority()) {
            path_infos.clear();
            path_infos.add(path_info);
        } else if (path_info.getPriority() == path_infos.get(0).getPriority()) {
            /*
             * first - 归属至从源头开始首个来源(即离效果发生处最远的来源) last -
             * 归属至从源头开始最后一个来源(即离效果发生处最近的来源) equal - 所有踩中的来源均分效果 all -
             * 所有踩中的来源同时得到相同效果
             */
            if (attr_calc.equals("all")) {
                path_infos.add(path_info);
            } else if (attr_calc.equals("equal")) {
                // TODO 添加equal效果
            } else if (attr_calc.equals("first")) {
                if (path_info.getFirstTs() < path_infos.get(0).getFirstTs()) {
                    path_infos.clear();
                    path_infos.add(path_info);
                }
            } else { // 默认last
                if (path_info.getLastTs() > path_infos.get(0).getLastTs()) {
                    path_infos.clear();
                    path_infos.add(path_info);
                }
            }
        }
    }

    public long GetPlanPathFirstTs(int pos) {
        return path_infos.get(pos).getFirstTs();
    }

    public long GetPlanPathLastTs(int pos) {
        return path_infos.get(pos).getLastTs();
    }

    public int GetPlanPathPriority(int pos) {
        return path_infos.get(pos).getPriority();
    }

    /*
     * 复制当前节点的path_infos给新的path_infos
     */
    public void clonePathInfo(List<EffectOwnershipPathinfo> new_path_infos) {
        new_path_infos.clear();
        for (EffectOwnershipPathinfo path_info : path_infos) {
            EffectOwnershipPathinfo new_path_info = new EffectOwnershipPathinfo(path_info);
            new_path_infos.add(new_path_info);
        }
    }
}
