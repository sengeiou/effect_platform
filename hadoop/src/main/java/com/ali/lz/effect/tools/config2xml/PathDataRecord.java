package com.ali.lz.effect.tools.config2xml;

/**
 * path_config表中data字段记录
 * 
 * @author jiuling.ypf
 * 
 */
public class PathDataRecord {

    private String name;

    private String url;

    private int step;

    public PathDataRecord(String name, String url, int step) {
        this.name = name;
        this.url = url;
        this.step = step;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

}
