package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.xml.sax.SAXException;

import com.ali.lz.effect.exception.HoloConfigParserException;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue.TypeRef;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue.TypeRef.TypePathInfo;
import com.ali.lz.effect.utils.Constants;

public class EffectOutsideTradeOwnershipReducer extends MapReduceBase implements
        Reducer<TextPair, BytesWritable, Text, Text> {

    // 同优先级来源效果归属规则 ( key: plan_id, value: first/last/equal/all )
    private HashMap<Integer, String> plan_map = new HashMap<Integer, String>();

    // key：plan_id value: 待归属的访问日志节点
    private HashMap<Integer, EffectOwnershipProcessBusinessLog> process_map = new HashMap<Integer, EffectOwnershipProcessBusinessLog>();

    public void configure(JobConf conf) {
        String conf_files = conf.get(Constants.CONFIG_FILE_PATH);
        for (String conf_file : conf_files.split(",")) {
            if (conf_file.length() == 0) {
                continue;
            }
            HoloConfig config = new HoloConfig();
            try {
                config.loadFile(conf_file);
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
            int plan_id = config.plan_id;
            String attr_calc = config.attr_calc_method;
            if (attr_calc == null) {
                return;
            }
            plan_map.put(plan_id, attr_calc);
        }
    }

    @Override
    public void reduce(TextPair key, Iterator<BytesWritable> values, OutputCollector<Text, Text> output,
            Reporter reporter) throws IOException {

        if (plan_map.size() == 0) {
            throw new IOException();
        }
        process_map.clear();
        boolean needOutput = true;

        // 进入reduce的都是 trade_track_info相同的，按照ts倒序排列
        while (values.hasNext()) {
            BytesWritable nodeData = values.next();
            TreeNodeValue value = LzEffectProtoUtil
                    .deserialize(Arrays.copyOf(nodeData.getBytes(), nodeData.getLength()));
            if (value == null) {
                continue;
            }

            if (value.getLogType() == 0) { // 访问日志

                for (TypeRef type_ref : value.getTypeRefList()) { // 处理多个plan情况，理论上不发生

                    int plan_id = type_ref.getPlanId();
                    String attr_calc = plan_map.get(type_ref.getPlanId());
                    EffectOwnershipHoloTreeNode node = processAccess(value, type_ref);

                    EffectOwnershipProcessBusinessLog process = process_map.get(plan_id);
                    if (process == null) {
                        process = new EffectOwnershipProcessBusinessLog();
                        process_map.put(plan_id, process);
                    }
                    process.appendAccessLog(node, attr_calc, needOutput);
                }

            } else if (value.getLogType() == 4) { // 交易日志

                EffectOwnershipGmvNode gmv_node = new EffectOwnershipGmvNode(value);
                Iterator<EffectOwnershipProcessBusinessLog> it = process_map.values().iterator();
                while (it.hasNext()) {
                    EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it.next();
                    for (EffectOwnershipHoloTreeNode node : process.getNodes()) {
                        // 单独输出一条包含浏览信息的成交日志，不和浏览汇合，完成归属。
                        gmv_node.CopyFromHoloTreeNode(node);
                        gmv_node.calcEffects();
                        // set index_type=4
                        for (EffectOwnershipPathinfo path_info : gmv_node.path_infos) {
                            path_info.setIndex_type(4);
                        }
                        for (String result : gmv_node.toStringList()) {
                            output.collect(new Text(String.valueOf(gmv_node.plan_id)), new Text(result));
                        }
                    }
                }
            } else {
                continue;
            }
        }

        // 清空访问日志节点，为避免同一份日志在站内成交和站外成交两次输出，站外成交归属时只输出归属上的成交节点，不输出访问节点。
        Iterator<EffectOwnershipProcessBusinessLog> it = process_map.values().iterator();
        while (it.hasNext()) {
            EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it.next();
            process.clearNodes(needOutput);
        }
    }

    private EffectOwnershipHoloTreeNode processAccess(TreeNodeValue value, TypeRef type_ref) {
        EffectOwnershipHoloTreeNode node = new EffectOwnershipHoloTreeNode(value);
        String attr_calc = plan_map.get(type_ref.getPlanId());
        node.setPlanInfo(type_ref, attr_calc);

        for (TypePathInfo path_info : type_ref.getPathInfoList()) {
            node.setPlanPathInfo(path_info);
        }
        // set index_type=4
        for (EffectOwnershipPathinfo path_info : node.path_infos) {
            path_info.setIndex_type(4);
        }
        return node;
    }
}
