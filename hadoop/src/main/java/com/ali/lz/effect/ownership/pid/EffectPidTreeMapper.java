package com.ali.lz.effect.ownership.pid;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

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

import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.holotree.HoloTreeUtil;
import com.ali.lz.effect.ownership.pid.EffectPidTreeUtil.PidSrcReferType;
import com.ali.lz.effect.proto.LzEffectPidProtoUtil;
import com.ali.lz.effect.proto.LzEffectPidProto.PidNodeValue;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.DomainUtil;
import com.ali.lz.effect.utils.StringUtil;

public class EffectPidTreeMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

    private static int ACCESS_LOG_COLUMN_NUM = 11;

    // 存储频道id和规则映射
    private Map<Integer, EffectChannelRule> channelRulesMap = new HashMap<Integer, EffectChannelRule>();

    // 联盟频道中的特殊list场景
    private static String RM_LIST_REFER_PREFIX = "http://rm.taobao.com/album/list.htm";
    private static String RM_LIST_URL_PREFIX = "http://rm.taobao.com/album/detail.htm";

    @Override
    public void configure(JobConf conf) {
        // 解析频道id和规则
        String config_path = conf.get(Constants.CONFIG_FILE_PATH);
        String exec_date = conf.get("exec_date");
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
                        // 活动页规则类型
                        if (fields.length < 6)
                            continue;
                        Integer channelId = Integer.valueOf(fields[0]);
                        String channelRuleType = fields[1];
                        String channelName = fields[2];
                        String channelRule = fields[3];
                        String startTimestamp = fields[4];
                        String endTimestamp = fields[5];
                        String channelStatus = fields[6];
                        if ("1".equals(channelStatus)) {
                            if ("1".equals(channelRuleType)) {
                                EffectChannelRule effectChannelRule = new EffectChannelRule(channelId, channelName,
                                        EffectChannelRule.RuleType.URL_PREFIX_MATCH, channelRule);
                                channelRulesMap.put(channelId, effectChannelRule);
                            } else if ("2".equals(channelRuleType)) {
                                EffectChannelRule effectChannelRule = new EffectChannelRule(channelId, channelName,
                                        EffectChannelRule.RuleType.REGEX_MATCH, channelRule);
                                channelRulesMap.put(channelId, effectChannelRule);
                            }
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

    @Override
    public void map(Object key, Text value, OutputCollector<TextPair, BytesWritable> output, Reporter reporter)
            throws IOException {

        String line = value.toString();
        String[] fields = line.split(Constants.CTRL_A, -1);
        // 判断输入字段长度
        if (fields.length < ACCESS_LOG_COLUMN_NUM) {
            System.out.println("数据错误");
            reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.LOG_FORMAT_ERROR, 1);
            return;
        }
        String url = fields[1];
        String refer = fields[2];
        String shopId = fields[3];
        String auctionId = fields[4];

        PidNodeValue.Builder builder = PidNodeValue.newBuilder();
        builder.setTs(Long.parseLong(fields[0].trim()));
        builder.setUrl(url);
        builder.setRefer(refer);
        builder.setShopId(shopId);
        builder.setAuctionId(auctionId);
        builder.setUserId(fields[5]);
        builder.setCookie(fields[6]);
        builder.setCookie2(fields[8]);

        boolean isEffectPage = false;
        boolean referIsEffectPage = false;
        int channelId = 0;
        int referChannelId = 0;
        int pitId = 0;
        String pitDetail = "";

        String trueRefer = refer;
        // 提前修正refer以便提取来源信息
        if ((trueRefer = HoloTreeUtil.getTrueReferer(refer)) == null)
            trueRefer = HoloTreeUtil.unescape(refer);

        // 判断是否活动页并打标签
        for (Integer channel_id : channelRulesMap.keySet()) {
            if (isEffectPage && referIsEffectPage)
                break;
            EffectChannelRule effectChannelRule = channelRulesMap.get(channel_id);
            if (!isEffectPage && effectChannelRule.match(url)) {
                isEffectPage = true;
                channelId = channel_id;

                reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_PAGE, 1);
            }
            if (!referIsEffectPage && effectChannelRule.match(trueRefer)) {
                referIsEffectPage = true;
                referChannelId = channel_id;
                reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.REFER_IS_EFFECT_PAGE, 1);
                // 标记坑位类型：1.宝贝 2.List 3.店铺
                String keyword = "";
                if (auctionId.length() > 0) {
                    pitId = EffectPitType.ITEM_PIT;
                    pitDetail = auctionId;
                    reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_ITEM_PIT, 1);
                } else if (!trueRefer.startsWith(RM_LIST_REFER_PREFIX)
                        && ((keyword = StringUtil.getUrlKeyword(url, "q", "GB18030")).length() > 0 || (keyword = StringUtil
                                .getUrlKeyword(url, "keyword", "GB18030")).length() > 0)) {
                    pitId = EffectPitType.LIST_PIT;
                    pitDetail = Constants.FIELD_REPLACE_PATTERN.matcher(keyword).replaceAll("");
                    reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_LIST_PIT, 1);
                } else if (trueRefer.startsWith(RM_LIST_REFER_PREFIX) && url.startsWith(RM_LIST_URL_PREFIX)) {
                    pitId = EffectPitType.LIST_PIT;
                    pitDetail = "albumId " + StringUtil.getUrlParameter(url, "albumId");
                    reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_LIST_PIT, 1);
                } else if (shopId.length() > 0) {
                    pitId = EffectPitType.SHOP_PIT;
                    pitDetail = shopId;
                    reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_SHOP_PIT, 1);
                }

            }
        }

        builder.setChannelId(channelId);
        builder.setReferChannelId(referChannelId);
        builder.setIsEffectPage(isEffectPage);
        builder.setReferIsEffectPage(referIsEffectPage);
        builder.setPitId(pitId);
        builder.setPitDetail(pitDetail);

        // 标记频道lp页面属性
        if (channelId > 0 && channelId != referChannelId) {
            builder.setIsChannelLp(true);
            int endIndex = trueRefer.indexOf('?');
            if (endIndex <= 0)
                endIndex = trueRefer.length();
            String srcRefer = trueRefer.substring(0, endIndex);
            builder.setSrcRefer(srcRefer);

            String referDomain = DomainUtil.getDomainFromUrl(trueRefer, 1);
            if (referChannelId > 0) {
                String channelName = channelRulesMap.get(referChannelId).getChannelName();
                builder.setSrcReferType(channelName);
            } else {
                PidSrcReferType srcReferType;
                if (referDomain.isEmpty())
                    srcReferType = PidSrcReferType.EMPTY;
                else if (referDomain.equals("taobao.com") || referDomain.equals("etao.com")
                        || referDomain.equals("tmall.com"))
                    srcReferType = PidSrcReferType.INSIDE;
                else
                    srcReferType = PidSrcReferType.OUTSIDE;
                builder.setSrcReferType(srcReferType.toString());
            }
        }

        // 标记Pid属性信息
        String pid = StringUtil.getUrlParameter(url, "pid");
        if (pid.length() <= 0)
            pid = StringUtil.getUrlParameter(url, "refpid");
        builder.setPid(pid);
        String[] pidParameters = pid.split("_", -1);
        switch (pidParameters.length) {
        case 4:
            builder.setAdzoneId(pidParameters[3]);
        case 3:
            builder.setSiteId(pidParameters[2]);
        case 2:
            builder.setPubId(pidParameters[1]);
        }

        // 标记宝贝页信息
        if (auctionId.length() > 0) {
            String[] useful_extra = fields[9].split(Constants.CTRL_B, -1);
            for (String field : useful_extra) {
                String[] keyValue = field.split(Constants.CTRL_C, -1);
                if (keyValue.length != 2) {
                    continue;
                }
                if ("ali_refid".equals(keyValue[0]) && builder.getPitId() == EffectPitType.ITEM_PIT) {
                    builder.setAliRefid(keyValue[1].split(":", -1)[0]);
                } else if ("ali_trackid".equals(keyValue[0])) {
                    String ali_trackid = keyValue[1];
                    String urlDomainLevel1 = DomainUtil.getDomainFromUrl(url, 1);
                    String urlDomain = "";
                    try {
                        urlDomain = urlDomainLevel1.split("\\.")[0];
                    } catch (Exception e) {
                        urlDomain = "";
                    }
                    // P4P
                    if (ali_trackid.startsWith("1_")) {
                        String[] ali_trackid_infos = ali_trackid.split("_", -1);
                        if (ali_trackid_infos.length == 2) {
                            builder.setItemClickid(ali_trackid_infos[1]);
                        }
                        if (urlDomain.equals("tmall"))
                            builder.setItemType(EffectPidTreeUtil.PidItemType.TMALL_P4P.ordinal());
                        else if (urlDomain.equals("taobao"))
                            builder.setItemType(EffectPidTreeUtil.PidItemType.TAOBAO_P4P.ordinal());
                    } else if (ali_trackid.startsWith("2:")) {
                        // taoke
                        String[] ali_trackid_infos = ali_trackid.split(":", -1);
                        if (ali_trackid_infos.length == 3) {
                            builder.setItemClickid(ali_trackid_infos[2]);
                        }
                        if (urlDomain.equals("tmall"))
                            builder.setItemType(EffectPidTreeUtil.PidItemType.TMALL_CPS.ordinal());
                        else if (urlDomain.equals("taobao"))
                            builder.setItemType(EffectPidTreeUtil.PidItemType.TAOBAO_CPS.ordinal());
                    }

                }
            }

        }

        PidNodeValue node = builder.build();

        byte[] data = LzEffectPidProtoUtil.serialize(node);

        Text cookie = new Text(node.getCookie());
        Text timestamp = new Text(String.valueOf(node.getTs()));
        output.collect(new TextPair(cookie, timestamp), new BytesWritable(data));
    }
}
