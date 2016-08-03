package com.ali.lz.effect.ownership.etao;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.holotree.HoloTreeUtil;
import com.ali.lz.effect.proto.LzEffectETaoTreeProtoUtil;
import com.ali.lz.effect.proto.LzEffectETaoTreeProto.ETaoTreeNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.DomainUtil;

public class EffectETaoTreeMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

    private static int ACCESS_LOG_COLUMN_NUM = 11;
    private static Pattern etaoFilterPattern = Pattern.compile("^(http|https)://tao\\.etao\\.com.*");

    // 存储频道id和规则映射
    private Map<Integer, Pattern> channelMap = new HashMap<Integer, Pattern>();

    @Override
    public void configure(JobConf conf) {
        // 解析频道id和规则
        String config_path = conf.get(Constants.CONFIG_FILE_PATH);
        Path path = new Path(config_path);
        SequenceFile.Reader reader = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(config_path), conf);
            FileStatus[] fileStatuses = fs.listStatus(path);
            if (fileStatuses != null) {
                for (FileStatus status : fileStatuses) {
                    reader = new SequenceFile.Reader(fs, status.getPath(), conf);
                    Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    while (reader.next(key, value)) {
                        String[] fields = value.toString().split(Constants.CTRL_A);
                        // 1表示etao频道规则
                        if (fields[3].equals("1")) {
                            String channelId = fields[0];
                            String channelRule = fields[2];
                            Pattern channelPattern = Pattern.compile(channelRule);
                            channelMap.put(Integer.valueOf(channelId), channelPattern);
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println(e.getStackTrace());
        } finally {
            IOUtils.closeStream(reader);
        }

    }

    /**
     * 只处理url或refer为etao的日志
     */
    public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
            throws IOException {
        String line = value.toString();
        String[] fields = line.split(Constants.CTRL_A, -1);
        // 判断输入字段长度
        if (fields.length < ACCESS_LOG_COLUMN_NUM) {
            System.out.println("数据错误");
            return;
        }

        String url = fields[1];
        String refer = fields[2];
        String trueRefer = refer;
        String tradeTrackInfo = "";

        boolean isEtao = false;
        boolean referIsEtao = false;

        boolean isLp = false;
        boolean referIsLp = false;
        String lpDomainName = "";
        ETaoSourceType lpSourceType = new ETaoSourceType();

        boolean isChannelLp = false;
        boolean referIsChannelLp = false;
        int channelId = 0;
        int referChannelId = 0;
        ETaoSourceType channelSourceType = new ETaoSourceType();

        // 提前修正refer以便提取来源信息
        if ((trueRefer = HoloTreeUtil.getTrueReferer(refer)) == null)
            trueRefer = HoloTreeUtil.unescape(refer);
        // 由于联盟二跳页面（tao.etao.com）与一淘网站整体没有什么业务关系，遂将其在一淘网站的所有效果数据中剔除
        if (Constants.etaoPattern.matcher(url).find() && !etaoFilterPattern.matcher(url).find()) {
            isEtao = true;
            tradeTrackInfo = EffectETaoTreeUtil.parseTradeTrackInfo(url, trueRefer, fields[4]);
            // 判断是否频道页并打标签
            for (Integer currChannelId : channelMap.keySet()) {
                Pattern channelPattern = channelMap.get(currChannelId);
                if (channelId <= 0 && channelPattern.matcher(url).find()) {
                    channelId = currChannelId;
                }
                if (referChannelId <= 0 && channelPattern.matcher(trueRefer).find()) {
                    referChannelId = currChannelId;
                }
            }
            // 频道lp页: 自身是频道页，且和上一跳不是相同频道
            if (channelId > 0 && channelId != referChannelId) {
                channelSourceType = EffectETaoTreeUtil.parseSrc(url, trueRefer, ETaoSourceType.CHANNEL_SRC_TYPE);
                isChannelLp = true;
            }

            // 判断是否为landing page并打标签
            if (!Constants.etaoPattern.matcher(trueRefer).find()) {
                isLp = true;
                lpDomainName = DomainUtil.getDomainFromUrl(url, 2);
                // 判断landing page的来源类型
                if (channelSourceType.getSrc_id() == 0)
                    lpSourceType = EffectETaoTreeUtil.parseSrc(url, trueRefer, ETaoSourceType.LP_SRC_TYPE);
                else
                    lpSourceType = channelSourceType;
            } else {
                referIsEtao = true;
            }
        } else if (Constants.etaoPattern.matcher(trueRefer).find() && !etaoFilterPattern.matcher(trueRefer).find()) {
            referIsEtao = true;
            // 判断refer是否频道页并打标签
            for (Integer currChannelId : channelMap.keySet()) {
                Pattern channelPattern = channelMap.get(currChannelId);
                if (channelPattern.matcher(trueRefer).find()) {
                    referChannelId = currChannelId;
                    break;
                }
            }
        } else {
            // 过滤掉所有url非etao, refer非etao的日志
            return;
        }

        ETaoTreeNodeValue.Builder builder = ETaoTreeNodeValue.newBuilder();

        builder.setTs(Long.parseLong(fields[0].trim()));
        builder.setUrl(fields[1]);
        builder.setRefer(fields[2]);
        builder.setShopId(fields[3]);
        builder.setAuctionId(fields[4]);
        builder.setUserId(fields[5]);
        // 建树时会根据logEntry中sid的值来判断是否按session截断, etao子树不按session截断
        builder.setSid("");
        builder.setCookie(fields[8]);
        String[] useful_extra = fields[9].split(Constants.CTRL_B, -1);
        for (String field : useful_extra) {
            String[] keyValue = field.split(Constants.CTRL_C, -1);
            if (keyValue.length != 2) {
                continue;
            }
            if ("adid".equals(keyValue[0])) {
                builder.setChannelAdid(keyValue[1]);
                builder.setLpAdid(keyValue[1]);
                builder.setRefChannelAdid(keyValue[1]);
            }
        }
        builder.setIsEtao(isEtao);
        builder.setRefIsEtao(referIsEtao);
        builder.setIsLp(isLp);
        builder.setRefIsLp(referIsLp);
        builder.setChannelId(channelId);
        builder.setRefChannelId(referChannelId);
        builder.setIsChannelLp(isChannelLp);
        builder.setRefIsChannelLp(referIsChannelLp);
        builder.setLpDomainName(lpDomainName);
        builder.setTradeTrackInfo(tradeTrackInfo);
        LzEffectETaoTreeProtoUtil.setETaoTreeNodeBuilderBySrc(channelSourceType, lpSourceType, builder);
        ETaoTreeNodeValue node = builder.build();

        byte[] data = LzEffectETaoTreeProtoUtil.serialize(node);

        Text sessionId = new Text(fields[6]);
        Text timestamp = new Text(String.valueOf(node.getTs()));
        output.collect(new TextPair(sessionId, timestamp), new BytesWritable(data));
    }
}
