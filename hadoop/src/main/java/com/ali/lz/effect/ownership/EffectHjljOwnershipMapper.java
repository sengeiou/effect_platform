package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.exception.URLMatcherException;
import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.holotree.HoloTreeUtil;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.utils.Constants;

public class EffectHjljOwnershipMapper {

    private static TextPair key = new TextPair();

    private static void MapOutput(OutputCollector<TextPair, Text> output, Text value, String logtype, Reporter reporter)
            throws IOException {

        String line = value.toString();
        String[] fields = line.split(Constants.CTRL_A, -1);
        // 判断输入字段长度
        if (fields.length < Constants.ELogColumnNum.ACCESS_LOG_COLUMN_NUM.getColumnNum()) {
            System.out.println("数据错误");
            reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.LOG_FORMAT_ERROR, 1);
            return;
        }

        String timestamp = fields[0].trim();
        String url = fields[1];
        String cookie = fields[6];
        key.setFirstText(cookie + "_" + url);
        key.setSecondText(timestamp);
        output.collect(key, new Text(logtype + Constants.CTRL_A + line));
    }

    public static class AccessLogMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, Text> {

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, Text> output, Reporter reporter)
                throws IOException {
            MapOutput(output, value, "0", reporter);
        }

    }

    public static class HjljLogMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, Text> {

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, Text> output, Reporter reporter)
                throws IOException {
            MapOutput(output, value, "1", reporter);
        }

    }

}
