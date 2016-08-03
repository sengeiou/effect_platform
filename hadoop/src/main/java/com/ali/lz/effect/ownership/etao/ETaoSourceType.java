package com.ali.lz.effect.ownership.etao;

import java.util.HashMap;
import java.util.Map;

public class ETaoSourceType {

    // etao网站11类来源id
    public static final int TB_MARKET_ID = 100;
    public static final int PID = 101;
    public static final int ETAO = 115;
    public static final int TB_LM_ID = 200;
    public static final int TB_EDM_ID = 201;
    public static final int TB_SEM_SITE = 202;
    public static final int SEO = 203;
    public static final int OUTSIDE = 204;
    public static final int TAOBAO = 205;
    public static final int TMALL = 206;
    public static final int SELF_INPUT = 207;

    // 来源属性类型：lp来源、频道一跳来源、频道二跳来源
    public static final int LP_SRC_TYPE = 1;
    public static final int CHANNEL_SRC_TYPE = 2;
    public static final int REF_CHANNEL_SRC_TYPE = 3;

    private Map<String, String> properties = new HashMap<String, String>();
    private int src_id;
    private int src_type;

    public int getSrc_type() {
        return src_type;
    }

    public void setSrc_type(int src_type) {
        this.src_type = src_type;
    }

    public ETaoSourceType() {
        this.src_id = 0;
    }

    public ETaoSourceType(int src_type) {
        this.src_id = 0;
        this.src_type = src_type;
    }

    public String getSourceProperty(String sourcePropertyName) {
        if (properties.containsKey(sourcePropertyName))
            return properties.get(sourcePropertyName);
        else
            return "";
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public void addProperty(String key, String value) {
        this.properties.put(key, preProcessValue(value));
    }

    public int getSrc_id() {
        return src_id;
    }

    public void setSrc_id(int src_id) {
        this.src_id = src_id;
    }

    public String preProcessValue(String value) {
        if (value == null || value.length() == 0) {
            value = "-";
        } else {
            value.replaceAll("\\r|\\n|\\001|\\t", "");
        }
        return value;
    }

}
