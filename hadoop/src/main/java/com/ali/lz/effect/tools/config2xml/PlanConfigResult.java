package com.ali.lz.effect.tools.config2xml;

/**
 * plan_config表查询结果
 * 
 * @author jiuling.ypf
 * 
 */
public class PlanConfigResult {

    private int planId;

    private int userId;

    private String effectUrl;

    private String srcType;

    private int pathType;

    private int belongId;

    private String indIds;

    private int period;

    private int expireType;

    private String linkType;

    public PlanConfigResult(int planId, int userId, String effectUrl, String srcType, int pathType, int belongId,
            String indIds, int period, int expireType, String linkType) {
        this.planId = planId;
        this.userId = userId;
        this.srcType = srcType;
        this.pathType = pathType;
        this.effectUrl = effectUrl;
        this.belongId = belongId;
        this.indIds = indIds;
        this.period = period;
        this.expireType = expireType;
        this.linkType = linkType;
    }

    public int getPlanId() {
        return planId;
    }

    public void setPlanId(int planId) {
        this.planId = planId;
    }

    public int getUserId() {
        return userId;
    }

    public void setUserId(int userId) {
        this.userId = userId;
    }

    public String getEffectUrl() {
        return effectUrl;
    }

    public void setEffectUrl(String effectUrl) {
        this.effectUrl = effectUrl;
    }

    public String getSrcType() {
        return srcType;
    }

    public void setSrcType(String srcType) {
        this.srcType = srcType;
    }

    public int getPathType() {
        return pathType;
    }

    public void setPathType(int pathType) {
        this.pathType = pathType;
    }

    public int getBelongId() {
        return belongId;
    }

    public void setBelongId(int belongId) {
        this.belongId = belongId;
    }

    public String getIndIds() {
        return indIds;
    }

    public void setIndIds(String indIds) {
        this.indIds = indIds;
    }

    public int getPeriod() {
        return period;
    }

    public void setPeriod(int period) {
        this.period = period;
    }

    public int getExpireType() {
        return expireType;
    }

    public void setExpireType(int expireType) {
        this.expireType = expireType;
    }

    public String getLinkType() {
        return linkType;
    }

    public void setLinkType(String linkType) {
        this.linkType = linkType;
    }
}
