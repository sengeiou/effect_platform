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
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue.TypeRef;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue.TypeRef.TypePathInfo;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.utils.Constants;

public class EffectOwnershipReducer extends MapReduceBase implements Reducer<TextPair, BytesWritable, Text, Text> {

    // 同优先级来源效果归属规则 ( key: plan_id, value: first/last/equal/all )
    private HashMap<Integer, String> plan_map = new HashMap<Integer, String>();
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

        boolean needOutputAccessLog = false;
        while (values.hasNext()) {
            BytesWritable nodeData = values.next();
            TreeNodeValue value = LzEffectProtoUtil
                    .deserialize(Arrays.copyOf(nodeData.getBytes(), nodeData.getLength()));
            if (value == null) {
                continue;
            }

            String auction_id = value.getAuctionId();

            if (value.getLogType() == Constants.ELogType.ACCESS_LOG.getLogType()) {
                if (key.getFirst().toString().startsWith(Constants.EGroupingKeyType.ACCESS_LOG.getPrefix())) {
                    needOutputAccessLog = true;
                }
                for (TypeRef type_ref : value.getTypeRefList()) { // 处理多个plan情况

                    int plan_id = type_ref.getPlanId();
                    EffectOwnershipHoloTreeNode node = processAccess(value, type_ref);

                    if (needOutputAccessLog) {
                        for (String result : node.toStringList()) {
                            output.collect(new Text(String.valueOf(plan_id)), new Text(result));
                        }
                    } else {
                        EffectOwnershipProcessBusinessLog process = process_map.get(plan_id);
                        if (process == null) {
                            process = new EffectOwnershipProcessBusinessLog(output);
                            process_map.put(plan_id, process);
                        }

                        String attr_calc = plan_map.get(type_ref.getPlanId());
                        process.appendAccessLog(node, attr_calc, needOutputAccessLog);
                    }
                }

            } else if (value.getLogType() == Constants.ELogType.GMV_LOG.getLogType()) { // 交易日志

                EffectOwnershipGmvNode gmv_node = new EffectOwnershipGmvNode(value);

                Iterator<EffectOwnershipProcessBusinessLog> it = process_map.values().iterator();
                while (it.hasNext()) {
                    EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it.next();
                    for (EffectOwnershipHoloTreeNode node : process.getNodes()) {
                        // 单独输出一条成交日志，不和浏览汇合。
                        gmv_node.CopyFromHoloTreeNode(node);
                        gmv_node.calcEffects();
                        for (String result : gmv_node.toStringList()) {
                            output.collect(new Text(String.valueOf(gmv_node.plan_id)), new Text(result));
                        }
                    }

                }
            } else if (value.getLogType() == Constants.ELogType.COLLECT_LOG.getLogType()) { // 收藏日志
                // 计算宝贝收藏和店铺收藏
                // TODO 修正归属逻辑

                EffectOwnershipCollectNode collect_node = new EffectOwnershipCollectNode(value);

                Iterator<EffectOwnershipProcessBusinessLog> it = process_map.values().iterator();
                while (it.hasNext()) {
                    EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it.next();
                    for (EffectOwnershipHoloTreeNode node : process.getNodes()) {
                        // 单独输出一条收藏日志，不和浏览汇合。
                        collect_node.CopyFromHoloTreeNode(node);
                        collect_node.calcEffects();
                        for (String result : collect_node.toStringList()) {
                            output.collect(new Text(String.valueOf(collect_node.plan_id)), new Text(result));
                        }
                    }
                }
            } else if (value.getLogType() == Constants.ELogType.CART_LOG.getLogType()) { // 购物车日志

                EffectOwnershipCartNode cart_node = new EffectOwnershipCartNode(value);

                Iterator<EffectOwnershipProcessBusinessLog> it = process_map.values().iterator();
                while (it.hasNext()) {
                    EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it.next();
                    for (EffectOwnershipHoloTreeNode node : process.getNodes()) {
                        // 单独输出一条成交日志，不和浏览汇合。
                        cart_node.CopyFromHoloTreeNode(node);
                        cart_node.calcEffects();
                        for (String result : cart_node.toStringList()) {
                            output.collect(new Text(String.valueOf(cart_node.plan_id)), new Text(result));
                        }
                    }
                }

            } else {
                continue;
            }
        }

        // 输出剩余的日志
        Iterator<EffectOwnershipProcessBusinessLog> it = process_map.values().iterator();
        while (it.hasNext()) {
            EffectOwnershipProcessBusinessLog process = (EffectOwnershipProcessBusinessLog) it.next();
            process.clearNodes(needOutputAccessLog);
        }
    }

    private EffectOwnershipHoloTreeNode processAccess(TreeNodeValue value, TypeRef type_ref) {
        EffectOwnershipHoloTreeNode node = new EffectOwnershipHoloTreeNode(value);
        String attr_calc = plan_map.get(type_ref.getPlanId());
        node.setPlanInfo(type_ref, attr_calc);

        for (TypePathInfo path_info : type_ref.getPathInfoList()) {
            node.setPlanPathInfo(path_info);
        }
        return node;
    }
}
