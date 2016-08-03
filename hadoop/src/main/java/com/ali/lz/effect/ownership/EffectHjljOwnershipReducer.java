package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;

public class EffectHjljOwnershipReducer extends MapReduceBase implements Reducer<TextPair, Text, Text, Text> {

    private List<String> accessLog = new ArrayList<String>();
    Text outputKey = new Text("");
    // 限制同一个用户在同一url下点击的黄金令箭节点次数，过滤刷票行为
    private int hjljLimit = 100;

    /**
     * 标记黄金令箭tag
     */
    public void reduce(TextPair key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
            throws IOException {

        String value = null;
        String[] fields = null;
        String logtype = null;
        int hjljCounter = 0;

        while (values.hasNext()) {
            value = values.next().toString();
            fields = value.split(Constants.CTRL_A, -1);
            logtype = fields[0];
            if (logtype.equals("0")) {
                if (!accessLog.isEmpty()) {
                    output.collect(outputKey, new Text(StringUtil.join(accessLog, Constants.CTRL_A)));
                    accessLog.clear();
                    hjljCounter = 0;
                }
                for (int i = 1; i < fields.length; i++) {
                    accessLog.add(fields[i]);
                }
            } else if (logtype.equals("1")) {
                reporter.incrCounter(EffectJobStatusCounter.EffectHjljOwnershipStatus.INPUT_HJLJ_LOGS, 1);
                if (!accessLog.isEmpty()) {
                    if (hjljCounter < hjljLimit) {
                        String useful_extra = fields[10];
                        String log_userful_extra = accessLog.get(9);
                        reporter.incrCounter(EffectJobStatusCounter.EffectHjljOwnershipStatus.INHERIT_HJLJ_LOGKEYS, 1);
                        accessLog.set(9, inheritHjljKeys(log_userful_extra, useful_extra));
                        hjljCounter++;
                    }
                }

            } else
                continue;
        }
        if (!accessLog.isEmpty()) {
            output.collect(outputKey, new Text(StringUtil.join(accessLog, Constants.CTRL_A)));
            accessLog.clear();
            hjljCounter = 0;
        }

    }

    public static String inheritHjljKeys(String useful_extras, String hjljLogKey) {
        Map<String, String> kvs = StringUtil.splitStr(useful_extras, Constants.CTRL_B, Constants.CTRL_C);
        String logkey = null;
        if ((logkey = kvs.get(Constants.HJLJ_LOGKEY)) != null) {
            Map<String, String> hjljKvs = StringUtil.splitStr(hjljLogKey, Constants.CTRL_B, Constants.CTRL_C);
            if (hjljKvs.containsKey(Constants.HJLJ_LOGKEY)) {
                logkey = logkey + Constants.CTRL_D + hjljKvs.get(Constants.HJLJ_LOGKEY);
            }
            kvs.put(Constants.HJLJ_LOGKEY, logkey);
            List<String> usefulExtraList = new ArrayList<String>();
            for (Entry<String, String> entry : kvs.entrySet()) {
                usefulExtraList.add(entry.getKey() + Constants.CTRL_C + entry.getValue());
            }
            return StringUtil.join(usefulExtraList, Constants.CTRL_B);
        } else {
            if (useful_extras.isEmpty())
                return hjljLogKey;
            else
                return useful_extras + Constants.CTRL_B + hjljLogKey;
        }
    }

}
