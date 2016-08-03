package com.etao.lz.effect;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.etao.lz.dw.util.StringUtil;

public class HoloTreeUtil {

	/**
	 * 将页面类型 ID 变换为适用于匹配的单字符
	 * 
	 * @param pageTypeId
	 *            页面类型 ID
	 * @return 变换后的单字符
	 */
	public static char pageTypeIdToChar(int pageTypeId) {
		return (char) (pageTypeId + 256);
	}

	/**
	 * 修改给定页面类型 root path 串，将最后一个节点的页面类型更改为新类型
	 * 
	 * @param ptRootPath
	 *            待修改的页面类型 root path 串
	 * @param newPageType
	 *            替换 root path 中最后一个节点的新页面类型
	 * @return 修改后的 root path
	 */
	public static String replacePTRootPathLast(String ptRootPath,
			int newPageType) {
		StringBuilder sb = new StringBuilder();
		sb.append(ptRootPath, 0, ptRootPath.length() - 1);
		sb.append(pageTypeIdToChar(newPageType));
		return sb.toString();
	}

	// 把key-value格式的字符串解析成map
	public static Map<String, String> splitStr(String str,
			String firstLevelSeparator, String secondLevelSeparator) {
		Map<String, String> strMap = new HashMap<String, String>();

		String[] strArray = str.split(firstLevelSeparator);

		int size = strArray.length;
		for (int i = 0; i < size; i++) {
			String[] currentPairArray = strArray[i].split(secondLevelSeparator,
					-1);

			if (currentPairArray.length == 2) {
				strMap.put(currentPairArray[0], currentPairArray[1]);
			}

		}

		return strMap;
	}

	public static String getUrlParameter(String url, String key) {
		String value = "";
		try {
			int pos = url.indexOf('?');
			if (pos != -1) {
				String queryString = url.substring(pos + 1);
				Map<String, String> kvPairs = splitStr(queryString, "&", "=");
				if (kvPairs.containsKey(key)) {
					value = URLDecoder.decode(kvPairs.get(key), "utf-8");
				}
			}
		} catch (Exception e) {
		}
		return value;
	}

	/**
	 * 解码给定 url 中 %uXXXX 和 %XX 形式的转义序列
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
			pos = src.indexOf('%', lastPos);
			if (pos == lastPos) {
				try {
					if (src.charAt(pos + 1) == 'u') {
						// 处理 %uXXXX 形式的转义序列
						ch = (char) Integer.parseInt(
								src.substring(pos + 2, pos + 6), 16);
						tmp.append(ch);
						lastPos = pos + 6;
					} else {
						// 处理 %XX 形式的转义序列
						int index = Integer.parseInt(
								src.substring(pos + 1, pos + 3), 16);
						lastPos = pos + 3;

						if (index < 128) {
							// 为了避免解开中文字符引起编码问题，这里仅对英文符号进行解转义处理
							ch = (char) index;
							if (ch == '%') {
								// 部分refer的参数中会有两次escape的情况，例如'+'两次escape后变为'%2F2B'，这里尝试进行二次解转义
								try {
									index = Integer
											.parseInt(src.substring(pos + 3,
													pos + 5), 16);
									if (index < 128) {
										// 二次转义后仍然为英文符号，跳过转义时消耗的字符
										ch = (char) index;
										lastPos += 2;
									}
								} catch (Exception e) {
									// 首次解析后的 % 之后并非合法的 2 位 16 进制数，保留该 %
									// 字符且不消耗其后的字符
								}
							}
							tmp.append(ch);
						} else {
							tmp.append(src.substring(pos, pos + 3));
						}
					}
				} catch (Exception e) {
					// %u 之后并非合法的 4 位 16 进制数，或 % 之后并非合法的 2 位 16 进制数，保留原有字符
					tmp.append(src.charAt(lastPos));
					lastPos += 1;
				}
			} else {
				if (pos == -1) {
					tmp.append(src.substring(lastPos));
					break;
				} else {
					tmp.append(src.substring(lastPos, pos));
					lastPos = pos;
				}

			}
		}

		return tmp.toString().replaceAll("\\r|\\n|\\t", "");
	}

	/**
	 * 替换URL中 \\xUU 和 \\uUUUU 为 %xx 和 %uxxxx 形式
	 * 
	 * @param url
	 * @return
	 */
	public static String transformUrl(String url) {
		StringBuffer tmp = new StringBuffer();
		tmp.ensureCapacity(url.length());
		int lastPos = 0, pos = 0;

		while (lastPos < url.length()) {
			pos = url.indexOf('\\', lastPos);

			if (pos == lastPos) {
				try {
					// \\x替换为%
					if (url.charAt(pos + 1) == 'x') {
						tmp.append("%"
								+ url.substring(pos + 2, pos + 4).toUpperCase());
						lastPos = pos + 4;
					}
					// \\u替换为%u,在unescape中会进一步处理
					else if (url.charAt(pos + 1) == 'u') {
						tmp.append("%"
								+ url.substring(pos + 1, pos + 4).toUpperCase());
						lastPos = pos + 4;
					} else {
						tmp.append(url.substring(pos, pos + 2));
						lastPos = pos + 2;
					}
				} catch (IndexOutOfBoundsException e) {
					tmp.append(url.charAt(lastPos));
					lastPos += 1;
				}
			} else {
				if (pos == -1) {
					tmp.append(url.substring(lastPos));
					lastPos = url.length();
				} else {
					tmp.append(url.substring(lastPos, pos));
					lastPos = pos;
				}
			}

		}
		return tmp.toString();
	}

	public static String getDomainFromUrl(String url, int domainLevel) {
		// level 1表示取一级域名 2表示取二级域名
		if (url == null) {
			return null;
		}

		String sourceUrl = url.toString();
		// http://aaa.bbb.taobao.com/aaa.html?a=2&b=4
		if (sourceUrl.startsWith("http://")) {
			sourceUrl = sourceUrl.substring(7);
		}

		if (sourceUrl.startsWith("http%3A//")) {
			sourceUrl = sourceUrl.substring(9);
		}

		// aaa.bbb.taobao.com/aaa.html?a=2&b=4
		int i = sourceUrl.indexOf("/");
		if (i < 0) {
			i = sourceUrl.length();
		}
		// aaa.bbb.taobao.com
		sourceUrl = sourceUrl.substring(0, i);
		String yuming = "";
		String houzhui = "";
		if (sourceUrl.endsWith(".com")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 4);
			houzhui = ".com";
		} else if (sourceUrl.endsWith(".tel")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 4);
			houzhui = ".tel";
		} else if (sourceUrl.endsWith(".mobi")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 4);
			houzhui = ".mobi";
		} else if (sourceUrl.endsWith(".net")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 4);
			houzhui = ".net";
		} else if (sourceUrl.endsWith(".org")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 4);
			houzhui = ".org";
		} else if (sourceUrl.endsWith(".asia")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 5);
			houzhui = ".asia";
		} else if (sourceUrl.endsWith(".me")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 3);
			houzhui = ".me";
		} else if (sourceUrl.endsWith(".com.cn")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 7);
			houzhui = ".com.cn";
		} else if (sourceUrl.endsWith(".net.cn")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 7);
			houzhui = ".net.cn";
		} else if (sourceUrl.endsWith(".org.cn")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 7);
			houzhui = ".org.cn";
		} else if (sourceUrl.endsWith(".gov.cn")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 7);
			houzhui = ".gov.cn";
		} else if (sourceUrl.endsWith(".hk")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 3);
			houzhui = ".hk";
		} else if (sourceUrl.endsWith(".tv")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 3);
			houzhui = ".tv";
		} else if (sourceUrl.endsWith(".biz")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 4);
			houzhui = ".biz";
		} else if (sourceUrl.endsWith(".cc")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 3);
			houzhui = ".cc";
		} else if (sourceUrl.endsWith(".name")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 5);
			houzhui = ".name";
		} else if (sourceUrl.endsWith(".info")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 5);
			houzhui = ".info";
		} else if (sourceUrl.endsWith(".cn")) {
			yuming = sourceUrl.substring(0, sourceUrl.length() - 3);
			houzhui = ".cn";
		}

		// yuming的值 = aaa.bbb.taobao
		String[] strList = yuming.split("\\.");

		String returnResult = "";
		if (domainLevel == 1) {
			String yuming1 = strList[strList.length - 1];
			yuming1 = yuming1 + houzhui;
			returnResult = yuming1;
		} else if (domainLevel == 2) {
			if (strList.length == 1) {
				yuming = strList[0];
			} else if (strList.length == 2) {
				yuming = strList[0] + "." + strList[1];
			} else if (strList.length > 2) {
				yuming = strList[strList.length - 2] + "."
						+ strList[strList.length - 1];
			}

			// 最后得到2级域名
			String yuming2 = yuming + houzhui;
			Pattern p = Pattern.compile("shop[0-9]+.taobao.com",
					Pattern.MULTILINE);
			if (p.matcher(yuming2).find()) {
				yuming2 = "shopXX.taobao.com";
			}
			returnResult = yuming2;
		}

		return returnResult;
	}

	static final String TAOKE_CLICK_REFERER_PREFIX = "http://s.click.taobao.com/t_js";
	static final String TAOKE_CLICK_REFERER_PREFIX2 = "http://item8.taobao.com/t_js";
	static final String TAOKE_CLICK_REFERER_PREFIX3 = "http://shop8.taobao.com/t_js";

	static final String TAOKE_CLICK_JS_REFER_KEY = "ref";
	static final String TAOKE_CLICK_JS_TU_KEY = "tu";
	static final String ADZONE_CLICK_REFERER_PREFIX = "http://z.alimama.com/alimama.php";
	static final String ADZONE_CLICK_REFERER_REF = "u";

	// 处理广告的一些特殊情况，refer不正确，需要从refer中的一个参数取refer
	public static String getTrueReferer(String refer) {
		// 淘客的点击串，此时refer为http://s.click.taobao.com/t_js?xxx
		// 真实 的refer为t_js中tu参数中的ref参数
		if (refer.startsWith(TAOKE_CLICK_REFERER_PREFIX)
				|| refer.startsWith(TAOKE_CLICK_REFERER_PREFIX2)
				|| refer.startsWith(TAOKE_CLICK_REFERER_PREFIX3)) {
			String true_refer = getUrlParameter(
					getUrlParameter(refer, TAOKE_CLICK_JS_TU_KEY),
					TAOKE_CLICK_JS_REFER_KEY);

			// 提取出的refer中关键字之间默认以‘+’分隔，替换为空格
			if (true_refer != null && !true_refer.equals("")) {
				// 提取参数后需要再次unescape
				true_refer = unescape(true_refer);
				int queryIndex = true_refer.indexOf('?');
				if (queryIndex != -1 && queryIndex < (true_refer.length() -1)) {
					StringBuffer buffer = new StringBuffer(200);
					buffer.append(true_refer.substring(0, queryIndex + 1));
					String queryString = true_refer.substring(queryIndex + 1);
					String[] strArray = queryString.split("&");
					for (int i = 0; i < strArray.length; i++) {
						if (strArray[i].startsWith("q=")) {
							strArray[i] = strArray[i].replace('+', ' ');
						}
					}
					buffer.append(StringUtil.join(strArray, "&"));
					return buffer.toString();
				} else {
					return true_refer;
				}
//				return true_refer.replace('+', ' ');
			} else
				return true_refer;
		}
		// 从alimama广告位访问过来,refer为http://z.alimama.com/alimama.php?
		// 真实的refer为页面refer的u参数的值
		if (refer.startsWith(ADZONE_CLICK_REFERER_PREFIX)) {
			return getUrlParameter(refer, ADZONE_CLICK_REFERER_REF);
		}

		return null;
	}
}
