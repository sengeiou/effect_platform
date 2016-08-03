package com.ali.lz.effect.extendutils;

import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;

public class MRUnitTools {

    public static Configuration getConfigurationfromMap(Map<String, String> conf_prop) {
        Configuration conf = new Configuration();
        if (!conf_prop.isEmpty()) {
            for (Entry<String, String> prop : conf_prop.entrySet()) {
                String value = "";
                /**
                 * comment this block to use relevant resource path other than
                 * target/test-classes/ files.
                 */
                // if (Constants.CONFIG_FILE_PATH.equals(prop.getKey())) {
                // for (String file : prop.getValue().split(",", -1)) {
                // value += ClassLoader.getSystemResource(file).getFile() + ',';
                // }
                // } else {
                // value = prop.getValue();
                // }
                value = prop.getValue();
                conf.set(prop.getKey(), value);
            }
        }
        return conf;
    }

}
