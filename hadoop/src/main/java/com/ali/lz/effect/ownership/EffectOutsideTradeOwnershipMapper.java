package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.ownership.etao.EffectETaoTreeUtil;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.DomainUtil;
import com.ali.lz.effect.utils.StringUtil;

public class EffectOutsideTradeOwnershipMapper {

    public static class AccessMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        private static int ACCESS_LOG_COLUMN_NUM = 17;

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < ACCESS_LOG_COLUMN_NUM) {
                return;
            }
            TreeNodeValue node = LzEffectProtoUtil.fromString(line);
            if (node == null) {
                return;
            }

            // 如果有染色，才output
            for (TreeNodeValue.TypeRef type_ref : node.getTypeRefList()) {
                if (type_ref.getPathInfoCount() > 0) {

                    // 解析trade_track_info
                    String trade_track_info = EffectETaoTreeUtil.parseTradeTrackInfo(node.getUrl(), node.getRefer(),
                            node.getAuctionId());

                    TreeNodeValue.Builder builder = TreeNodeValue.newBuilder(node);
                    TreeNodeValue.KeyValueS.Builder access_useful_extra_builder = TreeNodeValue.KeyValueS.newBuilder();
                    access_useful_extra_builder.setKey("trade_track_info");
                    access_useful_extra_builder.setValue(trade_track_info);
                    builder.addAccessUsefulExtra(access_useful_extra_builder);
                    node = builder.build();

                    byte[] data = LzEffectProtoUtil.serialize(node);
                    // 按规则输出， 提取trade_track_info
                    Text group_id = new Text(trade_track_info);
                    Text timestamp = new Text(String.valueOf(node.getTs()));
                    output.collect(new TextPair(group_id, timestamp), new BytesWritable(data));
                }
            }
        }

    }

    public static class OutsideTradeMapper extends MapReduceBase implements
            Mapper<Object, Text, TextPair, BytesWritable> {

        private static int OT_LOG_COLUMN_NUM = 10;

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < OT_LOG_COLUMN_NUM) {
                return;
            }

            TreeNodeValue.Builder builder = TreeNodeValue.newBuilder();
            TreeNodeValue.KeyValueS.Builder access_useful_extra_builder = TreeNodeValue.KeyValueS.newBuilder();

            builder.setLogType(4); // 站外成交日志类型为4
            builder.setTs(Long.parseLong(fields[0]));
            access_useful_extra_builder.setKey("trade_track_info");
            access_useful_extra_builder.setValue(fields[2]);
            builder.addAccessUsefulExtra(access_useful_extra_builder);
            if (fields[4] != null)
                builder.setAuctionId(fields[4]);
            else
                builder.setAuctionId("");
            builder.setUserId(fields[5]);
            builder.setGmvTradeNum(Float.parseFloat(fields[6]));
            builder.setGmvAmt(Float.parseFloat(fields[7]));
            builder.setAlipayTradeNum(Float.parseFloat(fields[8]));
            builder.setAlipayAmt(Float.parseFloat(fields[9]));

            // TreeNode build
            TreeNodeValue node = builder.build();
            if (node == null) {
                return;
            }

            // 序列化为二进制文本
            byte[] data = LzEffectProtoUtil.serialize(node);

            if (node.getTs() > 0) {
                // 按规则输出
                Text group_id = new Text(fields[2]);
                Text timestamp = new Text(String.valueOf(node.getTs()));
                output.collect(new TextPair(group_id, timestamp), new BytesWritable(data));
            }
        }
    }
}
