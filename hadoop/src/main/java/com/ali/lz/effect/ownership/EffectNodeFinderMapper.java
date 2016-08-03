package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.xml.sax.SAXException;

import com.ali.lz.effect.exception.HoloConfigParserException;
import com.ali.lz.effect.exception.URLMatcherException;
import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.holotree.HoloTreeUtil;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.holotree.URLMatcher;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.utils.Constants;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.taobao.lz.dms.utils.UrlFilter;

public class EffectNodeFinderMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

    private class PlanMapValue {
        public int analyzerId = -1;
        public URLMatcher matcher = null;
    }

    private String treeSplitMethod = "none";
    private HashMap<Integer, PlanMapValue> planMap = new HashMap<Integer, PlanMapValue>();

    private List<String> groupingFields = new ArrayList<String>();

    private boolean doUrlMasking = false;

    @Override
    public void configure(JobConf conf) {
        // 获取配置xml文件名
        String confFiles = conf.get(Constants.CONFIG_FILE_PATH);
        // 创建matcher类
        for (String confFile : confFiles.split(",")) {
            if (confFile.length() == 0) {
                continue;
            }
            HoloConfig config = new HoloConfig();
            try {
                config.loadFile(confFile);
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
                return;
            } catch (SAXException e) {
                e.printStackTrace();
                return;
            } catch (IOException e) {
                e.printStackTrace();
                return;
            } catch (HoloConfigParserException e) {
                e.printStackTrace();
                return;
            }
            PlanMapValue value = new PlanMapValue();
            URLMatcher matcher;
            try {
                matcher = new URLMatcher(config);
                // 得到当前配置的rule_id
                int plan_id = config.plan_id;
                value.analyzerId = config.analyzer_id;
                value.matcher = matcher;
                // 切分树的属性，控制计算范围。TODO
                treeSplitMethod = config.tree_split_method;
                // 加入到plan_map中
                planMap.put(plan_id, value);
            } catch (IOException e) {
                e.printStackTrace();
            }

            groupingFields = config.treeGroupingFields;
            if (config.doUrlMasking) {
                doUrlMasking = true;
            }
            
        }
        
        if (doUrlMasking) {
            try {
                UrlFilter.init("/black_list.txt", "/white_list.txt");
            } catch (IOException e) {
                doUrlMasking = false;
            }
        }
    }

    public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
            throws IOException {

        if (planMap.size() == 0) {
            throw new IOException();
        }

        String line = value.toString();
        String[] fields = line.split(Constants.CTRL_A, -1);
        // 判断输入字段长度
        if (fields.length < Constants.ELogColumnNum.ACCESS_LOG_COLUMN_NUM.getColumnNum()) {
            System.out.println("数据错误");
            reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.LOG_FORMAT_ERROR, 1);
            return;
        }

        // 声明protocol buffer Builder
        TreeNodeValue.KeyValueI.Builder keyValueI_builder = TreeNodeValue.KeyValueI.newBuilder();
        TreeNodeValue.KeyValueS.Builder keyValueS_builder = TreeNodeValue.KeyValueS.newBuilder();
        TreeNodeValue.TypeRef.Builder type_builder = TreeNodeValue.TypeRef.newBuilder();
        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

        // 写入日志内容参数
        builder.setTs(Long.parseLong(fields[0].trim()));
        builder.setUrl(fields[1]);
        builder.setRefer(fields[2]);
        builder.setShopId(fields[3]);
        builder.setAuctionId(fields[4]);
        builder.setUserId(fields[5]);
        builder.setCookie(fields[6]);
        builder.setSession(fields[7]);
        builder.setVisitId(fields[8]);

        // 创建用来做Matcher操作的map结构
        Map<String, Object> match_map = new HashMap<String, Object>();
        match_map.put("url", builder.getUrl());
        match_map.put("refer_url", builder.getRefer());

        if (doUrlMasking) {
            URLMatcher.doUrlMasking(match_map);
        }

        String[] useful_extra = fields[9].split(Constants.CTRL_B, -1);
        for (String field : useful_extra) {
            String[] keyValue = field.split(Constants.CTRL_C, -1);
            if (keyValue.length != 2) {
                continue;
            }
            keyValueS_builder.setKey(keyValue[0]);
            keyValueS_builder.setValue(keyValue[1]);
            builder.addAccessUsefulExtra(keyValueS_builder);
            // access_useful_extra字段中的key、value会加入Matcher操作中
            match_map.put(keyValue[0], keyValue[1]);
        }
        builder.setAccessExtra(fields[10]);

        // 执行每个plan的Matcher引擎，将结果写入type_ref中
        for (Map.Entry<Integer, PlanMapValue> p_entry : planMap.entrySet()) {
            PlanMapValue m_value = p_entry.getValue();
            try {
                PTLogEntry result = m_value.matcher.grep(match_map);

                type_builder.setAnalyzerId(m_value.analyzerId);
                type_builder.setPlanId(p_entry.getKey());
                type_builder.setPtype(result.getPType());
                type_builder.setRtype(result.getRType());
                type_builder.setIsMatched(result.matched());

                for (Map.Entry<String, Object> captured : result.entrySet()) {
                    if (captured.getKey().equals("ali_corp")) {
                        builder.setAliCorp((Integer) captured.getValue());
                    } else {
                        keyValueS_builder.setKey(captured.getKey());
                        keyValueS_builder.setValue((String) captured.getValue());
                        type_builder.addCapturedInfo(keyValueS_builder);
                    }
                }
                for (Map.Entry<String, Integer> source : result.getSourceType().entrySet()) {
                    keyValueI_builder.setKey(source.getKey());
                    keyValueI_builder.setValue(source.getValue());
                    type_builder.addSourceInfo(keyValueI_builder);
                }

            } catch (URLMatcherException e) {
                e.printStackTrace();
            }
            builder.addTypeRef(type_builder);
        }

        // 按规则输出
        if (needTreeSplit(builder)) {
            TreeNodeValue node = builder.build();
            if (node == null) {
                return;
            }
            // 序列化为二进制文本
            byte[] data = LzEffectProtoUtil.serialize(node);

            Text groupingKey = new Text(HoloTreeUtil.wrapGroupingKey(node, groupingFields));
            Text timestamp = new Text(String.valueOf(node.getTs()));
            output.collect(new TextPair(groupingKey, timestamp), new BytesWritable(data));
        }
    }

    /**
     * TODO 根据路径拆分规则判断是否要拆分（核心逻辑尚未实现）
     * 
     * @param builder
     * @return
     */
    public Boolean needTreeSplit(TreeNodeValue.Builder builder) {
        Boolean b_out = true;
        if (treeSplitMethod.indexOf("ali:") >= 0) {
            switch (builder.getAliCorp()) {
            case 1:
                b_out = treeSplitMethod.indexOf("etao") == -1 ? false : true;
            case 2:
                b_out = treeSplitMethod.indexOf("taobao") == -1 ? false : true;
            case 3:
                b_out = treeSplitMethod.indexOf("tmall") == -1 ? false : true;
            case 4:
                b_out = treeSplitMethod.indexOf("jhs") == -1 ? false : true;
            default:
                b_out = true;
            }
        }
        return b_out;
    }
}
