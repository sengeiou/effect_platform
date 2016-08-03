package com.etao.data.ep.accesstree.util;

import java.io.UnsupportedEncodingException;
import com.taobao.loganalyzer.aplus.Aplus.AplusLog;

public class AplusAccessTreeUtil {
	/**
	 * 拼接url_info字段，与acookie日志兼容
	 * 
	 * @param aplusLog
	 * @return
	 */
	public static String getUrlInfo(AplusLog aplusLog) {
		if (aplusLog == null)
			return "";
		String title = aplusLog.getTitle().toStringUtf8();

		String pre;
		try {
			pre = java.net.URLEncoder.encode(aplusLog.getPre().toStringUtf8(),
					"utf-8");
		} catch (UnsupportedEncodingException e) {
			pre = aplusLog.getPre().toStringUtf8();
		}
		String category = aplusLog.getCategory().toStringUtf8();
		String userid = String.valueOf(aplusLog.getUserid() == 0 ? ""
				: aplusLog.getUserid());
		String at_autype = aplusLog.getAtAutype().toStringUtf8();
		String at_shoptype = aplusLog.getAtShoptype().toStringUtf8();

		if (!"".equals(at_autype)) {
			String result = "title=" + title + "&pre=" + pre + "&category="
					+ category + "&userid=" + userid + "&at_autype="
					+ at_autype;
			return result;
		} else {
			String result = "title=" + title + "&pre=" + pre + "&category="
					+ category + "&userid=" + userid + "&at_shoptype="
					+ at_shoptype;
			return result;
		}
	}

	/**
	 * 原始日志中采用网络字节序的整数，而不是点分十进制，现拆成点分割的正常ip地址
	 * 
	 * @param longIP
	 * @return
	 */
	public static String longToIP(long longIP) {
		StringBuffer sb = new StringBuffer("");

		sb.append(String.valueOf((long) (longIP & 0x000000FF)));
		sb.append(".");
		sb.append(String.valueOf((long) (longIP & 0x0000FFFF) >> 8));
		sb.append(".");
		sb.append(String.valueOf((long) (longIP & 0x00FFFFFF) >> 16));
		sb.append(".");
		long tmpLong = (long) longIP >> 24;
		if (tmpLong < 0)
			tmpLong += 256;
		sb.append(String.valueOf(tmpLong));

		return sb.toString();
	}

	/**
	 * 字符串编码
	 * 
	 * @param src
	 * @return
	 */
	public static String escape(String src) {
		int i;
		char j;
		StringBuffer tmp = new StringBuffer();
		tmp.ensureCapacity(src.length() * 6);
		for (i = 0; i < src.length(); i++) {
			j = src.charAt(i);
			if (Character.isDigit(j) || Character.isLowerCase(j)
					|| Character.isUpperCase(j))
				tmp.append(j);
			else if (j < 256) {
				tmp.append("%");
				if (j < 16)
					tmp.append("0");
				tmp.append(Integer.toString(j, 16));
			} else {
				tmp.append("%u");
				tmp.append(Integer.toString(j, 16));
			}
		}
		return tmp.toString();
	}

	/**
	 * 字符串解码
	 * 
	 * @param src
	 * @return
	 */
	public static String unescape(String src) {
		StringBuffer tmp = new StringBuffer();
		tmp.ensureCapacity(src.length());
		int lastPos = 0, pos = 0;
		char ch;
		while (lastPos < src.length()) {
			pos = src.indexOf("%", lastPos);
			if (pos == lastPos) {
				if (src.charAt(pos + 1) == 'u') {
					ch = (char) Integer.parseInt(
							src.substring(pos + 2, pos + 6), 16);
					tmp.append(ch);
					lastPos = pos + 6;
				} else {
					ch = (char) Integer.parseInt(
							src.substring(pos + 1, pos + 3), 16);
					tmp.append(ch);
					lastPos = pos + 3;
				}
			} else {
				if (pos == -1) {
					tmp.append(src.substring(lastPos));
					lastPos = src.length();
				} else {
					tmp.append(src.substring(lastPos, pos));
					lastPos = pos;
				}
			}
		}
		return tmp.toString();
	}

	public static void main(String[] args) {
		System.out
				.println(unescape("%u590D%u53E4%20%u8FDE%u8863%u88D9_%u6DD8%u5B9D%u641C%u7D22"));
		System.out.println(escape("复古 连衣裙_淘宝搜索"));
		System.out.println(longToIP(16777343));
	}

}
