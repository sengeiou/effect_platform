package com.ali.lz.effect.ownership.wireless;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.proto.LzEffectWirelessProtoUtil;
import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessNodeValue;
import com.ali.lz.effect.utils.Constants;

public class EffectWirelessOwnershipMapper {

    /**
     * map输出函数
     * 
     * @param output
     * @param node
     * @throws IOException
     */
    private static void MapOutput(OutputCollector<TextPair, BytesWritable> output, WirelessNodeValue node)
            throws IOException {

        byte[] data = LzEffectWirelessProtoUtil.serializeWirelessNodeValue(node);
        Text groupId;
        if (node.getUserId().length() > 0){
            groupId = new Text(node.getUserId() + "_" + node.getPlatformId());
        } else {
            groupId = new Text(String.valueOf(node.getTs()));
        }
        Text timestamp = new Text(String.valueOf(node.getTs()));
        output.collect(new TextPair(groupId, timestamp), new BytesWritable(data));
    }

    /**
     * 定义读取流量日志的mapper
     * 
     * @author nanjia.lj
     * 
     */
    public static class AccessMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

        private static int ACCESS_LOG_COLUMN_NUM = 15;

        @Override
        public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
                throws IOException {
            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            if (fields.length != ACCESS_LOG_COLUMN_NUM) {
                System.out.println("数据错误");
                return;
            }

            WirelessNodeValue.Builder builder = WirelessNodeValue.newBuilder();

            builder.setLogType(0); // 访问日志类型为0
            builder.setTs(Long.parseLong(fields[0]));
            builder.setPlatformId(fields[1]);
            builder.setAuctionId(fields[6]);
            builder.setShopId(fields[5]);
            builder.setUserId(fields[7]);
            builder.setIsEffectPage(fields[8].equals("0") ? false : true);
            builder.setReferIsEffectPage(fields[9].equals("0") ? false : true);
            builder.setPlanId(fields[10]);
            builder.setPitId(fields[11]);
            builder.setPitDetail(fields[12]);
            builder.setPositionId(fields[13]);
            builder.setUrl(fields[3]);
            builder.setRefer(fields[4]);
            builder.setCookie(fields[14]);

            WirelessNodeValue node = builder.build();
            if (node == null) {
                return;
            }

            if (node.getTs() > 0) {
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
            if (fields.length != GMV_LOG_COLUMN_NUM) {
                System.out.println("数据错误");
                return;
            }
            // 获取platform_id
            String platform_id;
            String platformid = fields[11].split(Constants.CTRL_B, -1)[0];
            String[] platformidkv = platformid.split(Constants.CTRL_C, -1);
            if (platformidkv[0].equals("platform_id")) {
                platform_id = platformidkv[1];
            } else
                return;

            WirelessNodeValue.Builder builder = WirelessNodeValue.newBuilder();

            builder.setLogType(1); // 交易日志类型为1
            builder.setTs(Long.parseLong(fields[0]));
            builder.setPlatformId(platform_id);
            builder.setAuctionId(fields[2]);
            builder.setShopId(fields[1]);
            builder.setUserId(fields[3]);
            builder.setGmvTradeNum(Integer.parseInt(fields[5]));
            builder.setGmvTradeAmt(Float.parseFloat(fields[6]));
            builder.setGmvAuctionNum(Integer.parseInt(fields[7]));
            builder.setAlipayTradeNum(Integer.parseInt(fields[8]));
            builder.setAlipayTradeAmt(Float.parseFloat(fields[9]));
            builder.setAlipayAuctionNum(Integer.parseInt(fields[10]));

            WirelessNodeValue node = builder.build();
            if (node == null) {
                return;
            }

            if (node.getTs() > 0 && node.getUserId().length() > 0 && node.getAuctionId().length() > 0) {
                // 在每个平台复制一份成交数据，避免大量成交和访问数据中无线标记错误导致无法归属的情况。
                builder.setPlatformId("1");
                MapOutput(output, builder.build());
                builder.setPlatformId("2");
                MapOutput(output, builder.build());
            }
        }
    }
}
