package com.ali.lz.effect.ownership.pid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import com.ali.lz.effect.utils.CollectionUtil;
import com.etao.lz.recollection.RegexSpaceMap;
import com.kenai.jaffl.annotations.In;

public class EffectChannelRule {

    private int channelId = 0;
    private String channelName = "";

    private String channelUrlPrefix = "";
    private Pattern channelRegexRule;

    private List<RegexSpaceMap<Integer>> regexSpaceMaps = new ArrayList<RegexSpaceMap<Integer>>();
    private Map<Integer, String> channelUrlPrefixs = new HashMap<Integer, String>();

    private RuleType channelRuleType = RuleType.URL_PREFIX_MATCH;

    public enum RuleType {
        URL_PREFIX_MATCH, REGEX_MATCH
    };

    public EffectChannelRule() {
    }

    public EffectChannelRule(int channelId, String channelName) {
        this.channelId = channelId;
        this.channelName = channelName;
    }

    public EffectChannelRule(int channelId, String channelName, RuleType ruleType, String channelRule) {
        this.channelId = channelId;
        this.channelName = channelName;
        this.setChannelRule(ruleType, channelRule);
    }

    public void putUrlPrefix(Integer channelId, String channelRule) {
        this.channelUrlPrefixs.put(channelId, channelRule);
    }

    public Collection<Integer> urlPrefixesMatch(String url) {
        if (url == null || url.isEmpty())
            return null;
        Set<Integer> keys = this.channelUrlPrefixs.keySet();
        if (keys != null) {
            Collection<Integer> matchedChannelIds = new HashSet<Integer>();
            for (Integer key : keys) {
                if (url.startsWith(this.channelUrlPrefixs.get(key)))
                    matchedChannelIds.add(key);
            }
            return matchedChannelIds;
        } else {
            return null;
        }

    }

    public Collection<Integer> matchAll(String url) {
        if (url == null || url.isEmpty()) {
            return null;
        }
        Collection<Integer> allMatchedChannelIds = urlPrefixesMatch(url);
        if (allMatchedChannelIds == null)
            return regexesMatch(url);
        else {
            Collection<Integer> regexesMatchedChannelIds = regexesMatch(url);
            if (regexesMatchedChannelIds != null)
                allMatchedChannelIds.addAll(regexesMatchedChannelIds);
            return allMatchedChannelIds;
        }
    }

    public void putRegex(Integer channelId, String channelRule) {
        channelRule = channelRulePreParser(channelRule);

        RegexSpaceMap<Integer> rm = null;
        if (regexSpaceMaps.isEmpty())
            rm = new RegexSpaceMap<Integer>();
        else {
            rm = regexSpaceMaps.get(regexSpaceMaps.size() - 1);
        }

        rm.put(channelRule, channelId);
        if (rm.size() > 100) {
            regexSpaceMaps.add(new RegexSpaceMap<Integer>());
        } else if (regexSpaceMaps.isEmpty()) {
            regexSpaceMaps.add(rm);
        }

    }

    public String channelRulePreParser(String channelRule) {
        if (!channelRule.endsWith(".*"))
            channelRule += ".*";

        if (channelRule.startsWith("^"))
            channelRule = channelRule.substring(1, channelRule.length());
        return channelRule;
    }

    public void Ready() {
        for (RegexSpaceMap<Integer> rm : regexSpaceMaps) {
            rm.fullCompact();
        }
    }

    public Collection<Integer> regexesMatch(String url) {
        Collection<Integer> matchRegexSet = null;
        for (RegexSpaceMap<Integer> rm : regexSpaceMaps) {
            Map<String, Integer> matchRegexMap = rm.spaceGetAssoc(url);
            if (matchRegexMap != null)
                matchRegexSet = CollectionUtil.mergeCollections(matchRegexSet, matchRegexMap.values());
        }
        return matchRegexSet;

    }

    public void setChannelRule(RuleType ruleType, String channelRule) {
        this.channelRuleType = ruleType;
        if (ruleType.equals(RuleType.URL_PREFIX_MATCH)) {
            this.channelUrlPrefix = channelRule;
        } else if (ruleType.equals(RuleType.REGEX_MATCH)) {
            this.channelRegexRule = Pattern.compile(channelRule);
        }
    }

    public boolean match(String url) {
        if (this.channelRuleType.equals(RuleType.URL_PREFIX_MATCH)) {
            return url.startsWith(channelUrlPrefix);
        } else if (this.channelRuleType.equals(RuleType.REGEX_MATCH)) {
            return this.channelRegexRule.matcher(url).find();
        } else {
            return false;
        }
    }

    public int getChannelId() {
        return channelId;
    }

    public String getChannelName() {
        return channelName;
    }

    public String getChannelUrlPrefix() {
        return channelUrlPrefix;
    }

    public Pattern getChannelRegexRule() {
        return channelRegexRule;
    }

    public RuleType getChannelRuleType() {
        return channelRuleType;
    }

}
