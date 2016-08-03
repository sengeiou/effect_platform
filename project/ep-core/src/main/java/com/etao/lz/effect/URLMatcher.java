package com.etao.lz.effect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.etao.lz.effect.exception.URLMatcherException;
import com.etao.lz.star.StarLogProtos;
import com.etao.lz.star.StarLogProtos.FlowStarLog;
import com.google.protobuf.Descriptors.FieldDescriptor;

// TODO: 匹配时，应该只是取值方法不同。重构合并两个grep方法。
public class URLMatcher {

	private static Pattern[] corpPatternCache = {
			Pattern.compile("^http://(.*\\.)?taobao\\.com.*"),
			Pattern.compile("^http://(.*\\.)?tmall\\.com.*"),
			Pattern.compile("^http://(.*\\.)?etao\\.com.*"),
			Pattern.compile("^http://ju.taobao\\.com.*"), };

	private static class RuleCache {
		public ArrayList<Pattern> match = new ArrayList<Pattern>();
		public ArrayList<Pattern> extract = new ArrayList<Pattern>();
	}

	private static final List<FieldDescriptor> flowStarLogFields = FlowStarLog
			.getDescriptor().getFields();

	private HoloConfig conf;
	private ArrayList<ArrayList<RuleCache>> patternCache = new ArrayList<ArrayList<RuleCache>>();

	/**
	 * @param conf
	 *            配置对象,此配置对象必须调用过load方法
	 * @return 无返回值
	 */
	public URLMatcher(HoloConfig conf) {
		this.conf = conf;
		prepare();
	}

	/**
	 * 匹配并捕获配置所指定的字段
	 * 
	 * @param logEntry
	 *            被匹配的日志对象
	 * @return PTLogEntry, 即匹配与捕获结果.
	 * @throws URLMatcherException
	 */
	public PTLogEntry doGrep(StarLogProtos.FlowStarLog logEntry)
			throws URLMatcherException {
		PTLogEntry rv = new PTLogEntry();
		// 5个分组一次来一遍
		int i = 0, j;
		for (ArrayList<HoloConfig.UrlRule> row : conf.getGroup()) {
			j = 0;
			for (HoloConfig.UrlRule rule : row) {
				String forMatch = (String) logEntry
						.getField(rule.match_field_fd);
				if (capture(rule, getPattern(i, j), forMatch, rv))
					break;
				j++;
			}
			i++;
		}
		// 判断所属公司
		i = 1;
		rv.put("ali_corp", new Integer(0));
		for (Pattern pattern : corpPatternCache) {
			if (pattern.matcher(logEntry.getUrl()).find()) {
				rv.put("ali_corp", new Integer(i));
			}
			i++;
		}
		return rv;
	}

	/**
	 * 匹配并捕获配置所指定的字段
	 * 
	 * @param logEntry
	 *            原始日志，这个对象的类由ProtocolBuffer生成
	 * @return PTLogEntry, 即匹配与捕获结果.
	 * @throws URLMatcherException
	 */
	public PTLogEntry grep(StarLogProtos.FlowStarLog logEntry)
			throws URLMatcherException {
		if (!logEntry.hasUrl())
			throw new URLMatcherException("`URL` field is not exist!");
		if (!logEntry.hasReferUrl())
			throw new URLMatcherException("`ReferURL` field is not exist!");
		PTLogEntry rv = doGrep(logEntry);
		mapLogEntry(logEntry, rv);
		return rv;
	}

	/**
	 * 离线计算用的接口，输入数据是个map
	 * 
	 * @param forMatch
	 * @return
	 * @throws URLMatcherException
	 */
	public PTLogEntry grep(Map<String, String> forMatch)
			throws URLMatcherException {
		PTLogEntry rv = new PTLogEntry();
		// 三个分组依次匹配一遍！
		int i = 0, j;
		for (ArrayList<HoloConfig.UrlRule> row : conf.getGroup()) {
			j = 0;
			for (HoloConfig.UrlRule rule : row) {
				if (forMatch.containsKey(rule.match_field)) {
					if (capture(rule, getPattern(i, j),
							forMatch.get(rule.match_field), rv))
						break;
				}
				j++;
			}
			i++;
		}
		// 判断所属公司
		i = 1;
		rv.put("ali_corp", new Integer(0));
		for (Pattern pattern : corpPatternCache) {
			if (forMatch.containsKey("url")
					&& pattern.matcher(forMatch.get("url")).find()) {
				rv.put("ali_corp", new Integer(i));
			}
			i++;
		}
		return rv;
	}

	/**
	 * 创建正则表达式的缓存，创建对象时自动会调用此函数。
	 * 
	 * @param 无
	 * @return 已编译的正则表达式数目
	 */
	private int prepare() {
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

	private boolean doCapture(final HoloConfig.MatchProfile profile,
			final Pattern pattern, final String source, int targetType,
			PTLogEntry rv) {
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

	private boolean capture(final HoloConfig.UrlRule rule,
			final RuleCache cache, final String source, PTLogEntry rv) {
		int i = 0;
		boolean matched = false;
		for (HoloConfig.MatchProfile profile : rule.match_regexps) {
			if (doCapture(profile, cache.match.get(i), source,
					rule.target_type, rv)) {
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
			doCapture(profile, cache.extract.get(i++), source,
					rule.target_type, rv);
		return true;
	}
}
