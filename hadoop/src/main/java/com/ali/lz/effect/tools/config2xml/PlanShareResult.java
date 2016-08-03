package com.ali.lz.effect.tools.config2xml;

/**
 * plan_share表查询结果
 * 
 * @author jiuling.ypf
 * 
 */
public class PlanShareResult {

    private int planId;

    private int type;

    public PlanShareResult(int planId, int type) {
        this.planId = planId;
        this.type = type;
    }

    public int getPlanId() {
        return planId;
    }

    public void setPlanId(int planId) {
        this.planId = planId;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }

}
