package com.etao.lz.effect;

import java.util.HashMap;

public class PTLogEntry extends HashMap<String, Object> {
	private static final long serialVersionUID = -707271103466066153L;

	private boolean ok = false;
	private int ptype_id = 0;
	private int rtype_id = 0;
	private HashMap<String, Integer> source = new HashMap<String, Integer>();

	public static final int CORP_UNKNOWN = 0; // 未知或非阿里系
	public static final int CORP_TAOBAO = 1; // 淘宝
	public static final int CORP_TMALL = 2; // 天猫
	public static final int CORP_ETAO = 3; // 一淘
	public static final int CORP_JU = 4; // 聚划算
	public static final int MAX_CORP = 5;

	public PTLogEntry() {
		super();

		// atpanel 中的 adid 字段实际上表明上一跳的信息，这里需要特别指定，否则获取 adid 字段的目标时没有数据
		source.put("adid", HoloConfig.MATCH_REFERER);
	}

	/**
	 * @return URL/ADID/REFERER是否被匹配到了
	 */
	public boolean matched() {
		return ok;
	}

	public void setMatched(boolean ok) {
		this.ok = ok;
	}

	/**
	 * @return type_id，如果没有被匹配到，返回0
	 */
	public int getPType() {
		// 未匹配到时 ptype_id 默认值就是 0
		return ptype_id;
	}

	public void setPType(int type_id) {
		this.ptype_id = type_id;
	}

	public int getRType() {
		// 未匹配到时 rtype_id 默认值就是 0
		return rtype_id;
	}

	public void setRType(int type_id) {
		this.rtype_id = type_id;
	}

	/**
	 * 返回某个被捕获的属性的target_type
	 * 
	 * @param key
	 *            属性的key
	 * @return HoloConfig.MATCH_NONE表示没有这个属性，否则返回属性值， HoloConfig.MATCH_REFERER:
	 *         referer HoloConfig.MATCH_URL: url
	 */
	public int getSourceType(String key) {
		Integer rv = source.get(key);
		return (rv == null) ? HoloConfig.MATCH_NONE : rv.intValue();
	}

	public void putSourceType(String key, int type) {
		source.put(key, new Integer(type));
	}

	public HashMap<String, Integer> getSourceType() {
		return source;
	}
}