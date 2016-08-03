package com.ali.lz.effect.holotree;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import au.com.bytecode.opencsv.CSVReader;

import com.ali.lz.effect.proto.StarLogProtos;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class GenericLoader {

    public static interface LogFmtDesc {
        String getLogFmt();

        void setLogPath(String logPath);

        boolean hasNext();

        String[] nextFields();

        void cleanup();
    }

    /**
     * 较为通用的测试数据加载接口
     * 
     * @param logPath
     *            测试数据相对或绝对路径
     * @param fmtDesc
     *            测试数据文件格式描述类
     * @param idxToPb
     *            指定每行不同下标字段
     * @return
     * @throws Exception
     */
    public static List<StarLogProtos.FlowStarLog> genericLoadTestDataToPB(String logPath, LogFmtDesc fmtDesc,
            Map<Integer, String> idxToPb) throws Exception {
        if (!logPath.startsWith("/")) {
            // 相对路径作为系统资源处理
            logPath = ClassLoader.getSystemResource(logPath).getFile();
        }

        // 建立PB字段名及描述符对应表，以便动态更新字段值
        Map<String, FieldDescriptor> pbFieldMap = new HashMap<String, FieldDescriptor>();
        List<FieldDescriptor> fields = StarLogProtos.FlowStarLog.getDescriptor().getFields();
        for (FieldDescriptor field : fields) {
            pbFieldMap.put(field.getName(), field);
        }

        StarLogProtos.FlowStarLog.Builder sfb = StarLogProtos.FlowStarLog.newBuilder();
        List<StarLogProtos.FlowStarLog> res = new LinkedList<StarLogProtos.FlowStarLog>();

        fmtDesc.setLogPath(logPath);
        while (fmtDesc.hasNext()) {
            sfb.clear();

            String[] l = fmtDesc.nextFields();
            for (int i = 0; i < l.length; i++) {
                String fname;
                if (idxToPb.containsKey(i)) {
                    fname = idxToPb.get(i);
                } else {
                    continue;
                }

                FieldDescriptor fdesc = pbFieldMap.get(fname);
                Object val = null;
                if (fname.equalsIgnoreCase("ts")) {
                    val = l[i].length() > 0 ? Long.parseLong(l[i]) * 1000L : 0; // 毫秒
                } else if (fname.equalsIgnoreCase("log_version")) {
                    val = Integer.parseInt(l[i]);
                } else {
                    val = l[i];
                }
                sfb.setField(fdesc, val);
            }

            // 若没有 puid 字段，这里拼一个进去
            if (!sfb.hasPuid()) {
                if (sfb.hasUid() && sfb.getUid().trim().length() > 0) {
                    sfb.setPuid(sfb.getUid() + ":");
                } else {
                    sfb.setPuid(":" + sfb.getSid());
                }
            }

            StarLogProtos.FlowStarLog logEntry = sfb.build();
            res.add(logEntry);
        }
        fmtDesc.cleanup();

        return res;
    }

    public static class RawLogFmtDesc implements LogFmtDesc {
        Scanner scanner;
        String encoding;
        String delimitRegex;

        public RawLogFmtDesc(String delRegex, String enc) {
            delimitRegex = delRegex;
            encoding = enc;
        }

        @Override
        public String getLogFmt() {
            return "raw";
        }

        @Override
        public boolean hasNext() {
            return scanner.hasNext();
        }

        @Override
        public String[] nextFields() {
            String line = scanner.nextLine();
            String[] res = line.split(delimitRegex);
            return res;
        }

        @Override
        public void setLogPath(String logPath) {
            try {
                scanner = new Scanner(new FileInputStream(logPath), encoding);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void cleanup() {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    public static class CsvLogFmtDesc implements LogFmtDesc {

        CSVReader reader;
        String[] line;
        String encoding;
        char separator = ',';
        char quotechar = '"';
        char escape = '\\';
        int skiplines = 1;

        public CsvLogFmtDesc(String enc, int skip) {
            encoding = enc;
            skiplines = skip;
        }

        public CsvLogFmtDesc(String enc, int skip, char sep, char quote, char esc) {
            this(enc, skip);
            separator = sep;
            quotechar = quote;
            escape = esc;
        }

        @Override
        public String getLogFmt() {
            return "csv";
        }

        @Override
        public boolean hasNext() {
            try {
                line = reader.readNext();
                if (line != null) {
                    return true;
                }
            } catch (Exception e) {
            }
            return false;
        }

        @Override
        public String[] nextFields() {
            return line;
        }

        @Override
        public void setLogPath(String logPath) {
            try {
                InputStreamReader isReader = new InputStreamReader(new FileInputStream(logPath), encoding);
                reader = new CSVReader(isReader, separator, quotechar, escape, skiplines);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void cleanup() {
            if (reader != null) {
                try {
                    reader.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

    }

}
