package com.ali.lz.effect.holotree;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue.TypeRef;
import com.ali.lz.effect.rule.RuleSet;

/**
 * 加载配置集，用于多配置文件批量效果计算
 * 
 * @author feiqiong.dpf
 * 
 */
public class HoloSet {

    // 需要特殊处理的plan集合
    private Set<Integer> processPlans = new HashSet<Integer>();

    private Map<Integer, HoloKit> HoloKitMap = new HashMap<Integer, HoloKit>();

    public class HoloKit {
        public HoloTreeBuilder builder = null;
        public RuleSet ruleSet = null;
    }

    /**
     * 加载不同配置的holoTreeBuilder
     * 
     * @param builder
     */
    public void addHoloConfig(HoloConfig config) {
        HoloKit info = new HoloKit();
        info.builder = new HoloTreeBuilder(config);
        info.ruleSet = new RuleSet(config);
        HoloKitMap.put(config.plan_id, info);

    }

    public void clearPlans() {
        processPlans.clear();
    }

    /**
     * 添加待处理plan_id.
     * <p>
     * 添加逻辑: 
     *      对于is_all=true的plan，对整个List建树
     *      对于is_all=false的plan：
     *          1.对于有具体效果页的配置，只对匹配有效果页的List建树
     *          2.对于效果页是0的配置，对url matcher过的List建树
     * </p>
     * 
     * @param typeRefs
     */
    public void addProcessPlans(List<TypeRef> typeRefs) {
        for (TreeNodeValue.TypeRef typeRef : typeRefs) {
            int planId = typeRef.getPlanId();
            if (HoloKitMap.get(planId).builder.getHoloConfig().is_all) {
                processPlans.add(typeRef.getPlanId());
            } else {
                if (typeRef.getIsMatched()) {
                    Set<Integer> epIds = HoloKitMap.get(planId).ruleSet.getEffectPageSet();
                    if (!epIds.contains(0)) {
                        if (epIds.contains(typeRef.getPtype())) {
                            processPlans.add(typeRef.getPlanId());
                        }
                    } else
                        processPlans.add(typeRef.getPlanId());
                }
            }
        }

    }

    public Set<Integer> getProcessPlans() {
        return processPlans;
    }

    public Map<Integer, HoloKit> getHoloKitMap() {
        return HoloKitMap;
    }

}
