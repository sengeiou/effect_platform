package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.Constants.EGroupingKeyType;

public class EffectOwnershipMapper {

    private static TextPair key = new TextPair();
    private static BytesWritable value = new BytesWritable();

    private static void MapOutput(OutputCollector<TextPair, BytesWritable> output, TreeNodeValue node,
            EGroupingKeyType groupingKeyType) throws IOException {

        byte[] data = LzEffectProtoUtil.serialize(node);
        value.set(data, 0, data.length);
        assembleGroupingKey(node, groupingKeyType);
        output.collect(key, value);
    }

    private static void assembleGroupingKey(TreeNodeValue node, EGroupingKeyType groupingKeyType) {
        StringBuilder groupId = new StringBuilder(100);
        groupId.append(groupingKeyType.getPrefix());
        switch (groupingKeyType) {
        case ACCESS_LOG:
            groupId.append(node.getCookie());
            break;
        case GMV_OWNERSHIP:
            groupId.append(node.getUserId()).append("_").append(node.getAuctionId());
            break;
        case COLLECT_OWNERSHIP:
            groupId.append(node.getUserId()).append("_").append(node.getShopId());
            break;
        case CART_OWNERSHIP:
            groupId.append(node.getCookie()).append("_").append(node.getAuctionId());
            break;
        }

        key.setFirstText(groupId.toString());
        key.setSecondText(String.valueOf(node.getTs()));
    }

    public static class HoloTreeMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        private boolean needGmvOwnership = false;
        private boolean needCartOwnership = false;
        private boolean needCollectOwnership = false;

        public void configure(JobConf conf) {
            needGmvOwnership = conf.getBoolean("gmv_ownership", false);
            needCartOwnership = conf.getBoolean("cart_ownership", false);
            needCollectOwnership = conf.getBoolean("collect_ownership", false);
        }
        
        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {

            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < Constants.ELogColumnNum.HOLOTREE_LOG_COLUMN_NUM.getColumnNum()) {
                reporter.incrCounter(EffectJobStatusCounter.EffectOwnershipStatus.HOLOTREE_LOG_FORMAT_ERROR, 1);
                return;
            }

            TreeNodeValue node = null;
            try {
                node = LzEffectProtoUtil.fromString(line);
            } catch (Exception e) {
                // 暂时加保护，避免发现因乱码导致切分出错，任务无法完成。
                System.err.println(line);
                reporter.incrCounter(EffectJobStatusCounter.EffectOwnershipStatus.PB_DESERIALIZE_ERROR, 1);
                return;
            }

            if (node != null) {
                // 如果有染色，才output
                // TODO: 是否按配置文件规则输出？
                List<TreeNodeValue.TypeRef> type_ref_list = node.getTypeRefList();
                if (type_ref_list.isEmpty()) {
                } else if (type_ref_list.size() > 1) {
                    System.err.println("find one type_ref more than 1!");
                } else {
                    TreeNodeValue.TypeRef type_ref = type_ref_list.get(0);
                    if (type_ref.getPathInfoCount() > 0) {

                        MapOutput(output, node, EGroupingKeyType.ACCESS_LOG);

                        if (needGmvOwnership && node.getUserId().length() > 0 && node.getAuctionId().length() > 0) {
                            MapOutput(output, node, EGroupingKeyType.GMV_OWNERSHIP);
                        }
                        if (needCollectOwnership && node.getUserId().length() > 0 && node.getShopId().length() > 0) {
                            MapOutput(output, node, EGroupingKeyType.COLLECT_OWNERSHIP);
                        }
                        if (needCartOwnership && node.getCookie().length() > 0 && node.getAuctionId().length() > 0) {
                            MapOutput(output, node, EGroupingKeyType.CART_OWNERSHIP);
                        }
                    }
                }
            }
        }
    }

    public static class GmvMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < Constants.ELogColumnNum.GMV_LOG_COLUMN_NUM.getColumnNum()) {
                reporter.incrCounter(EffectJobStatusCounter.EffectOwnershipStatus.GMV_LOG_FORMAT_ERROR, 1);
                return;
            }

            builder.clear();
            builder.setLogType(Constants.ELogType.GMV_LOG.getLogType());
            builder.setTs(Long.parseLong(fields[0]));
            builder.setShopId(fields[1]);
            builder.setAuctionId(fields[2]);
            builder.setUserId(fields[3]);
            builder.setAliCorp(Integer.parseInt(fields[4]));

            builder.setGmvTradeNum(Float.parseFloat(fields[5]));
            builder.setGmvAmt(Float.parseFloat(fields[6]));
            builder.setGmvAuctionNum(Float.parseFloat(fields[7]));
            builder.setAlipayTradeNum(Float.parseFloat(fields[8]));
            builder.setAlipayAmt(Float.parseFloat(fields[9]));
            builder.setAlipayAuctionNum(Float.parseFloat(fields[10]));
            builder.setAccessExtra(fields[12]);

            TreeNodeValue node = builder.build();

            if (node != null && node.getTs() > 0 && node.getUserId().length() > 0 && node.getAuctionId().length() > 0
                    && !node.getAuctionId().equals("0")) {
                MapOutput(output, node, EGroupingKeyType.GMV_OWNERSHIP);
            }
        }

    }

    public static class CollectMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {

            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < Constants.ELogColumnNum.COLLECT_LOG_COLUMN_NUM.getColumnNum()) {
                reporter.incrCounter(EffectJobStatusCounter.EffectOwnershipStatus.COLLECT_LOG_FORMAT_ERROR, 1);
                return;
            }

            builder.clear();
            builder.setLogType(Constants.ELogType.COLLECT_LOG.getLogType());
            builder.setTs(Long.parseLong(fields[0]));
            builder.setShopId(fields[2]);
            builder.setAuctionId(fields[3]);
            builder.setUserId(fields[4]);
            builder.setAliCorp(Integer.parseInt(fields[5]));

            int type = Integer.parseInt(fields[1]);
            if (type == 0) {
                builder.setShopCollectNum(Float.parseFloat(fields[6]));
            } else if (type == 1) {
                builder.setItemCollectNum(Float.parseFloat(fields[6]));
            }

            TreeNodeValue node = builder.build();

            if (node != null && node.getTs() > 0 && node.getUserId().length() > 0 && node.getShopId().length() > 0) {
                MapOutput(output, node, EGroupingKeyType.COLLECT_OWNERSHIP);
            }

        }
    }

    public static class CartMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {

            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < Constants.ELogColumnNum.CART_LOG_COLUMN_NUM.getColumnNum()) {
                reporter.incrCounter(EffectJobStatusCounter.EffectOwnershipStatus.CART_LOG_FORMAT_ERROR, 1);
                return;
            }

            builder.clear();
            builder.setLogType(Constants.ELogType.CART_LOG.getLogType());
            builder.setTs(Long.parseLong(fields[0]));
            builder.setShopId(fields[1]);
            builder.setAuctionId(fields[2]);
            builder.setUserId(fields[3]);
            builder.setCookie(fields[4]);
            builder.setAliCorp(Integer.parseInt(fields[5]));
            builder.setCartNum(Float.parseFloat(fields[6]));

            TreeNodeValue node = builder.build();

            if (node != null && node.getTs() > 0 && node.getCookie().length() > 0 && node.getAuctionId().length() > 0
                    && !node.getAuctionId().equals("0")) {
                MapOutput(output, node, EGroupingKeyType.CART_OWNERSHIP);
            }
        }
    }
}
