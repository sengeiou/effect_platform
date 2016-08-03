package com.ali.lz.effect.tools.config2xml;

import java.util.List;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

/**
 * JSON操作类
 * 
 * @author jiuling.ypf
 * 
 */
public class JsonUtil {

    private static final Gson gson = new Gson();

    /**
     * 从PathDataRecord转换为json
     * 
     * @param pathList
     * @return
     */
    public static String toJson(List<PathDataRecord> pathList) {
        return gson.toJson(pathList);
    }

    /**
     * 从json转换成PathDataRecord
     * 
     * @param json
     * @return
     */
    public static List<PathDataRecord> fromJson(String json) {
        return gson.fromJson(json, new TypeToken<List<PathDataRecord>>() {
        }.getType());
    }

}
