package com.ali.lz.effect.ownership.pid;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.proto.LzEffectPidProtoUtil;
import com.ali.lz.effect.proto.LzEffectPidProto.PidNodeValue;
import com.ali.lz.effect.utils.Constants;

public class EffectPidOwnershipMapper {

    /**
     * map输出函数
     * 
     * @param output
     * @param node
     * @throws IOException
     */
    private static void MapOutput(OutputCollector<TextPair, BytesWritable> output, PidNodeValue node)
            throws IOException {

        byte[] data = LzEffectPidProtoUtil.serialize(node);
        Text user_id = new Text(node.getUserId());
        if (String.valueOf(user_id).length() < 1 || String.valueOf(user_id).equals("0")) {
            user_id = new Text(String.valueOf(node.getTs()));
        }
        Text timestamp = new Text(String.valueOf(node.getTs()));
        output.collect(new TextPair(user_id, timestamp), new BytesWritable(data));
    }

    /**
     * 定义读取流量日志的mapper
     * 
     * @author nanjia.lj
     * 
     */
    public static class AccessMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        private static int ACCESS_LOG_COLUMN_NUM = 28;

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < ACCESS_LOG_COLUMN_NUM) {
                System.out.println("数据错误");
                return;
            }

            PidNodeValue.Builder builder = PidNodeValue.newBuilder();

            builder.setLogType(0); // 访问日志类型为0
            try {
                builder.setTs(Long.parseLong(fields[0]));
                builder.setChannelId(Integer.parseInt(fields[2]));
                builder.setReferChannelId(Integer.parseInt(fields[6]));
            } catch (NumberFormatException e) {
                return;
            }
            builder.setPid(fields[3]);
            builder.setSrcRefer(fields[4]);
            builder.setSrcReferType(fields[5]);
            builder.setReferSrcRefer(fields[7]);
            builder.setReferSrcReferType(fields[8]);
            builder.setIsChannelLp(fields[9].equals("0") ? false : true);
            builder.setReferIsChannelLp(fields[10].equals("0") ? false : true);
            builder.setUrl(fields[11]);
            builder.setRefer(fields[12]);
            builder.setShopId(fields[13]);
            builder.setAuctionId(fields[14]);
            builder.setUserId(fields[15]);
            builder.setCookie(fields[16]);
            builder.setCookie2(fields[17]);
            builder.setIsEffectPage(fields[18].equals("0") ? false : true);
            builder.setReferIsEffectPage(fields[19].equals("0") ? false : true);
            try {
                builder.setPitId(Integer.parseInt(fields[20]));
            } catch (NumberFormatException e) {
                builder.setPitId(0);
            }
            builder.setPitDetail(fields[21]);
            try {
                builder.setItemType(Integer.parseInt(fields[22]));
            } catch (NumberFormatException e) {
                builder.setItemType(0);
            }
            builder.setItemClickid(fields[23]);
            builder.setAliRefid(fields[24]);
            builder.setPubId(fields[25]);
            builder.setSiteId(fields[26]);
            builder.setAdzoneId(fields[27]);

            PidNodeValue node = builder.build();
            if (node == null) {
                return;
            }

            if (node.getTs() > 0 && node.getUserId().length() > 0) {
                MapOutput(output, node);
            }
        }
    }

    /**
     * 定义读取成交日志的mapper
     * 
     * @author nanjia.lj
     * 
     */
    public static class GmvMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        private static int GMV_LOG_COLUMN_NUM = 13;

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length < GMV_LOG_COLUMN_NUM) {
                System.out.println("数据错误");
                return;
            }

            PidNodeValue.Builder builder = PidNodeValue.newBuilder();

            builder.setLogType(1); // 交易日志类型为1
            builder.setTs(Long.parseLong(fields[0]));
            builder.setAuctionId(fields[2]);
            builder.setShopId(fields[1]);
            builder.setUserId(fields[3]);
            builder.setGmvTradeNum(Integer.parseInt(fields[5]));
            builder.setGmvTradeAmt(Float.parseFloat(fields[6]));
            builder.setGmvAuctionNum(Integer.parseInt(fields[7]));
            builder.setAlipayTradeNum(Integer.parseInt(fields[8]));
            builder.setAlipayTradeAmt(Float.parseFloat(fields[9]));
            builder.setAlipayAuctionNum(Integer.parseInt(fields[10]));
            String[] extra = fields[12].split(Constants.CTRL_B, -1);
            builder.setOrderId(extra[0]);

            PidNodeValue node = builder.build();
            if (node == null) {
                return;
            }

            if (node.getTs() > 0 && node.getUserId().length() > 0 && node.getAuctionId().length() > 0) {
                MapOutput(output, node);
            }
        }
    }
}
