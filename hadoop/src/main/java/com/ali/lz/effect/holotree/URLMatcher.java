package com.ali.lz.effect.holotree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.ali.lz.effect.exception.URLMatcherException;
import com.ali.lz.effect.proto.StarLogProtos;
import com.ali.lz.effect.proto.StarLogProtos.FlowStarLog;
import com.ali.lz.effect.utils.Constants.AliCorpUrl;
import com.etao.lz.recollection.RegexSpaceMap;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.taobao.lz.dms.utils.UrlFilter;

// TODO: 正则匹配过程升级为recollection引擎
public class URLMatcher {

    private RegexSpaceMap<Integer> rsm = new RegexSpaceMap<Integer>();

    private static class RuleCache {
        public ArrayList<Pattern> match = new ArrayList<Pattern>();
        public ArrayList<Pattern> extract = new ArrayList<Pattern>();
    }

    private static final List<FieldDescriptor> flowStarLogFields = FlowStarLog.getDescriptor().getFields();

    private HoloConfig conf;
    private ArrayList<ArrayList<RuleCache>> patternCache = new ArrayList<ArrayList<RuleCache>>();

    /**
     * @param conf
     *            配置对象,此配置对象必须调用过load方法
     * @return 无返回值
     * @throws IOException
     */
    public URLMatcher(HoloConfig conf) throws IOException {
        this.conf = conf;
        prepare();
    }

    /**
     * 离线计算用的接口，输入数据是个map
     * 
     * @param forMatch
     * @return
     * @throws URLMatcherException
     */
    public PTLogEntry grep(Map<String, Object> forMatch) throws URLMatcherException {
        PTLogEntry rv = new PTLogEntry();

        for (int i = 0; i < conf.getGroup().size(); i++) {
            for (int j = 0; j < conf.getGroup(i).size(); j++) {
                HoloConfig.UrlRule urlRule = conf.getGroup(i).get(j);
                if (forMatch.containsKey(urlRule.match_field)) {
                    if (capture(urlRule, getPattern(i, j), (String) forMatch.get(urlRule.match_field), rv))
                        break;
                }
            }
        }
        // 判断所属公司
        rv.put("ali_corp", new Integer(0));
        if (forMatch.containsKey("url")) {
            Integer aliCorpId = rsm.spaceGet((String) forMatch.get("url"));
            if (aliCorpId != null) {
                rv.put("ali_corp", aliCorpId);
            }
        }
        return rv;
    }

    /**
     * 根据配置决定是否对url和refer进行脱敏
     * 
     * @param forMatch
     */
    public static void doUrlMasking(Map<String, Object> forMatch) {

        String maskingUrl = null;
        String maskingRefer = null;
        try {
            maskingUrl = UrlFilter.Filter((String) forMatch.get("url"), true);
        } catch (IOException e) {
            maskingUrl = (String) forMatch.get("url");
        }
        try {
            maskingRefer = UrlFilter.Filter((String) forMatch.get("refer_url"), true);
        } catch (IOException e) {
            maskingRefer = (String) forMatch.get("refer_url");
        }
        forMatch.put("url", maskingUrl);
        forMatch.put("refer_url", maskingRefer);
    }

    /**
     * 匹配并捕获配置所指定的字段
     * 
     * @param flowLog
     *            原始日志，这个对象的类由ProtocolBuffer生成
     * @return PTLogEntry, 即匹配与捕获结果.
     * @throws URLMatcherException
     */
    public PTLogEntry grep(StarLogProtos.FlowStarLog flowLog) throws URLMatcherException {
        if (!flowLog.hasUrl())
            throw new URLMatcherException("`URL` field is not exist!");
        if (!flowLog.hasReferUrl())
            throw new URLMatcherException("`ReferURL` field is not exist!");
        PTLogEntry logEntry = new PTLogEntry();
        mapLogEntry(flowLog, logEntry);
        PTLogEntry rv = grep(logEntry);
        mapLogEntry(flowLog, rv);
        return rv;
    }

    /**
     * 创建正则表达式的缓存，创建对象时自动会调用此函数。
     * 
     * @param 无
     * @return 已编译的正则表达式数目
     * @throws IOException
     */
    private int prepare() throws IOException {
        int count = 0;
        for (ArrayList<HoloConfig.UrlRule> group : conf.getGroup()) {
            ArrayList<RuleCache> row = new ArrayList<RuleCache>();

            for (HoloConfig.UrlRule rule : group) {
                RuleCache entry = new RuleCache();

                for (HoloConfig.MatchProfile profile : rule.match_regexps) {
                    entry.match.add(Pattern.compile(profile.regexp));
                    ++count;
                }
                for (HoloConfig.MatchProfile profile : rule.extract_regexps) {
                    entry.extract.add(Pattern.compile(profile.regexp));
                    ++count;
                }
                row.add(entry);
            }
            patternCache.add(row);
        }

        for (AliCorpUrl aliCorpUrl : AliCorpUrl.values()) {
            rsm.put(aliCorpUrl.getUrl(), aliCorpUrl.getId());
        }
        rsm.fullCompact();

        return count;
    }

    /**
     * 将StarLogProtos.FlowStarLog以键-值对的形式映射到PTLogEntry对象上。
     * 
     * @param logEntry
     * @param rv
     */
    private void mapLogEntry(FlowStarLog logEntry, PTLogEntry rv) {
        for (FieldDescriptor fd : flowStarLogFields)
            rv.put(fd.getName(), logEntry.getField(fd));
    }

    private RuleCache getPattern(int groupid, int i) {
        return patternCache.get(groupid).get(i);
    }

    private boolean capture(final HoloConfig.UrlRule rule, final RuleCache cache, final String source, PTLogEntry rv) {
        int i = 0;
        boolean matched = false;
        for (HoloConfig.MatchProfile profile : rule.match_regexps) {
            if (doCapture(profile, cache.match.get(i), source, rule.target_type, rv)) {
                // 设置Matching结果及ID
                rv.setMatched(true);
                if (rule.target_type == HoloConfig.MATCH_REFERER)
                    rv.setRType(rule.type_id);
                else if (rule.target_type == HoloConfig.MATCH_URL)
                    rv.setPType(rule.type_id);
                matched = true;
            }
            ++i;
        }
        if (matched == false)
            return false;
        // 额外捕获
        i = 0;
        for (HoloConfig.MatchProfile profile : rule.extract_regexps)
            doCapture(profile, cache.extract.get(i++), source, rule.target_type, rv);
        return true;
    }

    private boolean doCapture(final HoloConfig.MatchProfile profile, final Pattern pattern, final String source,
            int targetType, PTLogEntry rv) {
        Matcher matcher = pattern.matcher(source);
        if (matcher.find()) {
            // 开始捕获
            for (Entry<String, Integer> kv : profile.props.entrySet()) {
                rv.put(kv.getKey(), matcher.group(kv.getValue()));
                rv.putSourceType(kv.getKey(), targetType); // 设置被捕获字段的targetType。
            }
            return true;
        }
        return false;
    }
}
