package com.ali.lz.effect.holotree;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.yaml.snakeyaml.Yaml;

import com.ali.lz.effect.proto.StarLogProtos;
import com.ali.lz.effect.proto.StarLogProtos.FlowStarLog;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.Constants.AliCorpUrl;
import com.etao.lz.recollection.RegexSpaceMap;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class BenchmarkOnly {

    @SuppressWarnings("unchecked")
    static void benchURLMatcher() throws Exception {
        GZIPInputStream gis = new GZIPInputStream(ClassLoader.getSystemResourceAsStream("benchmark.yaml.gz"));

        long tic = System.currentTimeMillis();
        Yaml yaml = new Yaml();
        List<Map<String, Object>> lst = (List<Map<String, Object>>) yaml.load(gis);
        long tac1 = System.currentTimeMillis();
        System.out.println("load data (" + lst.size() + " recs, " + (tac1 - tic) / 1000.0 + " s)");

        StarLogProtos.FlowStarLog.Builder builder = StarLogProtos.FlowStarLog.newBuilder();
        List<FieldDescriptor> fields = StarLogProtos.FlowStarLog.getDescriptor().getFields();

        List<FlowStarLog> logs = new LinkedList<FlowStarLog>();
        for (Map<String, Object> m : lst) {
            builder.clear();

            for (FieldDescriptor fd : fields) {
                if (fd.getJavaType() == FieldDescriptor.JavaType.INT) {
                    builder.setField(fd, (Integer) m.get(fd.getName()));
                } else if (fd.getJavaType() == FieldDescriptor.JavaType.LONG) {
                    builder.setField(fd, (Long) m.get(fd.getName()));
                } else {
                    builder.setField(fd, m.get(fd.getName()));
                }
            }

            logs.add(builder.build());
        }
        long tac2 = System.currentTimeMillis();
        System.out.println("convert to pb (" + logs.size() + " recs, " + (tac2 - tac1) / 1000.0 + " s)");

        HoloConfig conf = new HoloConfig(ClassLoader.getSystemResource("benchmark.xml").getPath());
        URLMatcher matcher = new URLMatcher(conf);
        System.out.println("url matcher initialization (" + (System.currentTimeMillis() - tac2) / 1000.0 + " s)");
        tac2 = System.currentTimeMillis();

        List<PTLogEntry> entries = new LinkedList<PTLogEntry>();
        for (FlowStarLog log : logs) {
            PTLogEntry res = matcher.grep(log);
            entries.add(res);
        }
        long tac3 = System.currentTimeMillis();
        System.out.println("url match (" + entries.size() + " recs, " + (tac3 - tac2) / 1000.0 + " s)");

        HoloTreeBuilder htb = new HoloTreeBuilder(conf);
        int cnt = 0;
        for (PTLogEntry entry : entries) {
            htb.appendLog(entry);
            cnt++;
            if (cnt > 2000) {
                htb.flush();
            }
        }
        long tac4 = System.currentTimeMillis();
        System.out.println("tree building (" + entries.size() + " recs, " + (tac4 - tac3) / 1000.0 + " s)");
    }

    public static void main(String args[]) throws Exception {
        String classPath = System.getProperty("java.class.path");
        System.out.println("classpath: " + classPath);
        benchURLMatcher();
    }

}
