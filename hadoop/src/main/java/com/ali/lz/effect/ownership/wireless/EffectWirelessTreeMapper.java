package com.ali.lz.effect.ownership.wireless;

import java.io.IOException;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.regex.Matcher;
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

import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.ownership.pid.EffectChannelRule;
import com.ali.lz.effect.ownership.pid.EffectPitType;
import com.ali.lz.effect.proto.LzEffectWirelessProtoUtil;
import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessTreeNodeValue;
import com.ali.lz.effect.proto.LzEffectWirelessProto.WirelessTreeNodeValue.PlanProperty;
import com.ali.lz.effect.utils.CollectionUtil;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;
import com.etao.lz.recollection.RegexSpaceMap;

public class EffectWirelessTreeMapper extends MapReduceBase implements Mapper<Object, Text, TextPair, BytesWritable> {

    private static int ACCESS_LOG_COLUMN_NUM = 11;

    // 存储频道id和规则映射
    // 采用recollection批量匹配正则
    private EffectChannelRule effectChannelRule = new EffectChannelRule();
    private List<EffectChannelRule> complexChannelRules = new ArrayList<EffectChannelRule>();
    // 一阳指活动规则被组装过后在日志中存储的前缀
    private Pattern yyzUrlPre = Pattern.compile("(^(http|https)://yyz(\\.([a-z0-9A-Z]*))+\\.com/)");
    RegexSpaceMap<Integer> yyzUrlPreRSMap = new RegexSpaceMap<Integer>();

    /**
     * 一阳指配置的活动页url可能被重新组装然后记录到日志中，因此需要还原真实的活动页url来匹配活动id
     * 
     * @param url
     *            日志中待修正的url或refer
     * @param param
     *            需要从url或refer中提取的参数，一阳指活动中为参数"url"
     * @return 修正后的url或refer
     */
    private String getTrueEffectUrl(String url, String param) {
        if (yyzUrlPreRSMap.spaceGet(url) != null) {
            Matcher yyzMatcher = yyzUrlPre.matcher(url);
            if (yyzMatcher.find()) {
                String urlParam = StringUtil.getUrlParameter(url, param);
                if (urlParam.length() > 0)
                    return yyzMatcher.group() + urlParam;
            }
        }
        return url;
    }

    @Override
    public void configure(JobConf conf) {
        // 解析一阳指活动id和规则
        String config_path = conf.get(Constants.CONFIG_FILE_PATH);
        if (config_path == null)
            return;
        Path path = new Path(config_path);
        SequenceFile.Reader reader = null;
        try {
            FileSystem fs = FileSystem.get(URI.create(config_path), conf);
            FileStatus[] fileStatuses = fs.listStatus(path);
            if (fileStatuses != null) {

                DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                for (FileStatus status : fileStatuses) {
                    reader = new SequenceFile.Reader(fs, status.getPath(), conf);
                    Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
                    Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
                    int count = 0;
                    while (reader.next(key, value)) {
                        String[] fields = value.toString().split(Constants.CTRL_A, -1);

                        Integer channelId = Integer.valueOf(fields[0]);
                        String channelRuleType = fields[1];
                        String channelName = fields[2];
                        String channelRule = fields[3];
                        String startTimestamp = fields[4];
                        String endTimestamp = fields[5];
                        String channelStatus = fields[6];
                        String yyz_plan_id = fields[12];

                        if ("1".equals(channelStatus)) {
                            if ("1".equals(channelRuleType)) {
                                effectChannelRule.putUrlPrefix(channelId, channelRule);
                            } else if ("2".equals(channelRuleType)) {
                                if (yyz_plan_id.length() > 0 && !yyz_plan_id.equals(Constants.NULL)) {
                                    // 组装一阳指活动url正则，对于wap版活动会有分页分tab的情况
                                    channelRule = channelRule.replace(".html", "").replace(".", "\\.")
                                            + "(-[a-z0-9A-Z]){0,2}\\.html.*";
                                    effectChannelRule.putRegex(channelId, channelRule);
                                } else {
                                    EffectChannelRule complexChannelRule = new EffectChannelRule();
                                    complexChannelRule.putRegex(channelId, channelRule);
                                    complexChannelRule.Ready();
                                    complexChannelRules.add(complexChannelRule);
                                }
                            }
                        }
                    }
                    effectChannelRule.Ready();
                }
                yyzUrlPreRSMap.put("(http|https)://yyz(\\.([a-z0-9A-Z]*))+\\.com/.*", 0);
                yyzUrlPreRSMap.fullCompact();
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
        // TODO Auto-generated method stub
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
        String shop_id = fields[3];
        String auction_id = fields[4];
        String user_id = fields[5];

        ArrayList<PlanProperty> planProperties = colorizeEffectPage(reporter, url, refer, shop_id, auction_id);

        if (planProperties.isEmpty() && auction_id.length() == 0) {
            // 过滤掉所有url非活动页或refer非活动页且非宝贝页的日志
            return;
        }

        WirelessTreeNodeValue.Builder builder = WirelessTreeNodeValue.newBuilder();

        builder.setTs(Long.parseLong(fields[0].trim()));
        builder.setUrl(url);
        builder.setRefer(refer);
        builder.setShopId(shop_id);
        builder.setCookie(fields[6]);
        builder.setAuctionId(auction_id);
        builder.setUserId(user_id);
        builder.addAllPlanProperties(planProperties);
        String[] useful_extra = fields[9].split(Constants.CTRL_B, -1);
        for (String field : useful_extra) {
            String[] keyValue = field.split(Constants.CTRL_C, -1);
            if (keyValue.length != 2) {
                continue;
            }
            if ("platform_id".equals(keyValue[0])) {
                builder.setPlatformId(keyValue[1]);
            }
        }
        WirelessTreeNodeValue node = builder.build();

        byte[] data = LzEffectWirelessProtoUtil.serializeWirelessTreeNodeValue(node);

        // 需要按无线平台类型分组建树
        Text groupId= new Text(node.getCookie() + "_" + node.getPlatformId());
        Text timestamp = new Text(String.valueOf(node.getTs()));
        output.collect(new TextPair(groupId, timestamp), new BytesWritable(data));

    }

    private ArrayList<PlanProperty> colorizeEffectPage(Reporter reporter, String url, String refer, String shop_id,
            String auction_id) {

        String trueEffectUrl = getTrueEffectUrl(url, "url");
        String trueEffectRefer = getTrueEffectUrl(refer, "url");
        ArrayList<PlanProperty> planProperties = new ArrayList<PlanProperty>();

        // 判断是否活动页并打标签
        Collection<Integer> channelIds = effectChannelRule.matchAll(trueEffectUrl);
        Collection<Integer> complexChannelIds = null;
        for (EffectChannelRule complexChannelRule : complexChannelRules) {
            Collection<Integer> partComplexChannelIds = complexChannelRule.matchAll(trueEffectUrl);
            if (partComplexChannelIds != null && !partComplexChannelIds.isEmpty()) {
                complexChannelIds = CollectionUtil.mergeCollections(complexChannelIds, partComplexChannelIds);
            }
        }
        channelIds = CollectionUtil.mergeCollections(channelIds, complexChannelIds);
        if (channelIds != null && !channelIds.isEmpty()) {
            for (Integer channelId : channelIds) {
                reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_PAGE, 1);
                PlanProperty.Builder planPropertyBuilder = PlanProperty.newBuilder();
                planPropertyBuilder.setPlanId(channelId.toString());
                planPropertyBuilder.setIsEffectPage(true);
                planProperties.add(planPropertyBuilder.build());
            }
        }
        channelIds = effectChannelRule.matchAll(trueEffectRefer);
        complexChannelIds = null;
        for (EffectChannelRule complexChannelRule : complexChannelRules) {
            Collection<Integer> partComplexChannelIds = complexChannelRule.matchAll(trueEffectRefer);
            if (partComplexChannelIds != null && !partComplexChannelIds.isEmpty()) {
                complexChannelIds = CollectionUtil.mergeCollections(complexChannelIds, partComplexChannelIds);
            }
        }
        channelIds = CollectionUtil.mergeCollections(channelIds, complexChannelIds);
        if (channelIds != null && !channelIds.isEmpty()) {
            // 标记坑位类型：1.宝贝 2.List 3.店铺
            String pid_id = "";
            String pid_detail = "";
            if (auction_id.length() > 0) {
                pid_id = String.valueOf(EffectPitType.ITEM_PIT);
                pid_detail = auction_id;
                reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_ITEM_PIT, 1);
            } else if (shop_id.length() > 0) {
                pid_id = String.valueOf(EffectPitType.SHOP_PIT);
                pid_detail = shop_id;
                reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_SHOP_PIT, 1);
            } else {
                String keyword = StringUtil.getUrlParameter(url, "q");
                if (keyword.length() > 0) {
                    pid_id = String.valueOf(EffectPitType.LIST_PIT);
                    pid_detail = Constants.FIELD_REPLACE_PATTERN.matcher(keyword).replaceAll("");
                    reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.IS_EFFECT_LIST_PIT, 1);
                }
            }
            for (Integer channelId : channelIds) {
                reporter.incrCounter(EffectJobStatusCounter.TreeBuilderStatus.REFER_IS_EFFECT_PAGE, 1);
                PlanProperty.Builder planPropertyBuilder = PlanProperty.newBuilder();
                planPropertyBuilder.setPlanId(channelId.toString());
                planPropertyBuilder.setReferIsEffectPage(true);
                planPropertyBuilder.setPitId(pid_id);
                planPropertyBuilder.setPitDetail(pid_detail);
                planProperties.add(planPropertyBuilder.build());
            }
        }
        return planProperties;
    }

}
