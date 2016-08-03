package com.ali.lz.effect.holotree;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.ali.lz.effect.utils.StringUtil;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.GeneratedMessage;

public class HoloTreeUtil {

    /**
     * 将页面类型ID变换为适用于匹配的单字符
     * 
     * @param pageTypeId
     *            页面类型ID
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
    public static String replacePTRootPathLast(String ptRootPath, int newPageType) {
        StringBuilder sb = new StringBuilder();
        sb.append(ptRootPath, 0, ptRootPath.length() - 1);
        sb.append(pageTypeIdToChar(newPageType));
        return sb.toString();
    }

    // 把key-value格式的字符串解析成map
    public static Map<String, String> splitStr(String str, String firstLevelSeparator, String secondLevelSeparator) {
        Map<String, String> strMap = new HashMap<String, String>();

        String[] strArray = str.split(firstLevelSeparator);

        int size = strArray.length;
        for (int i = 0; i < size; i++) {
            String[] currentPairArray = strArray[i].split(secondLevelSeparator, -1);

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
                        ch = (char) Integer.parseInt(src.substring(pos + 2, pos + 6), 16);
                        tmp.append(ch);
                        lastPos = pos + 6;
                    } else {
                        // 处理 %XX 形式的转义序列
                        int index = Integer.parseInt(src.substring(pos + 1, pos + 3), 16);
                        lastPos = pos + 3;

                        if (index < 128) {
                            // 为了避免解开中文字符引起编码问题，这里仅对英文符号进行解转义处理
                            ch = (char) index;
                            if (ch == '%') {
                                // 部分refer的参数中会有两次escape的情况，例如'+'两次escape后变为'%2F2B'，这里尝试进行二次解转义
                                try {
                                    index = Integer.parseInt(src.substring(pos + 3, pos + 5), 16);
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

        int anchor_index;
        if ((anchor_index = tmp.toString().indexOf('#')) != -1)
            return URL_REPLACE_PATTERN.matcher(tmp.substring(0, anchor_index)).replaceAll("");
        return URL_REPLACE_PATTERN.matcher(tmp.toString()).replaceAll("");
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
                        tmp.append("%" + url.substring(pos + 2, pos + 4).toUpperCase());
                        lastPos = pos + 4;
                    }
                    // \\u替换为%u,在unescape中会进一步处理
                    else if (url.charAt(pos + 1) == 'u') {
                        tmp.append("%" + url.substring(pos + 1, pos + 4).toUpperCase());
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

    static final Pattern URL_REPLACE_PATTERN = Pattern.compile("\\r|\\n|\\t|\\001");
    static final String TAOKE_CLICK_REFERER_PREFIX = "http://s.click.taobao.com/t_js";
    static final String TAOKE_CLICK_REFERER_PREFIX2 = "http://item8.taobao.com/t_js";
    static final String TAOKE_CLICK_REFERER_PREFIX3 = "http://shop8.taobao.com/t_js";
    static final String TAOKE_CLICK_REFERER_PREFIX4 = "http://i.click.taobao.com/t_js";

    static final String TAOKE_CLICK_JS_REFER_KEY = "ref";
    static final String TAOKE_CLICK_JS_TU_KEY = "tu";
    static final String ADZONE_CLICK_REFERER_PREFIX = "http://z.alimama.com/alimama.php";
    static final String ADZONE_CLICK_REFERER_REF = "u";

    // 处理广告的一些特殊情况，refer不正确，需要从refer中的一个参数取refer
    public static String getTrueReferer(String refer) {
        // 淘客的点击串，此时refer为http://s.click.taobao.com/t_js?xxx
        // 真实 的refer为t_js中tu参数中的ref参数
        if (refer.startsWith(TAOKE_CLICK_REFERER_PREFIX) || refer.startsWith(TAOKE_CLICK_REFERER_PREFIX2)
                || refer.startsWith(TAOKE_CLICK_REFERER_PREFIX3) || refer.startsWith(TAOKE_CLICK_REFERER_PREFIX4)) {
            String true_refer = getUrlParameter(getUrlParameter(refer, TAOKE_CLICK_JS_TU_KEY), TAOKE_CLICK_JS_REFER_KEY);

            // 提取出的refer中关键字之间默认以‘+’分隔，替换为空格
            if (true_refer != null && !true_refer.equals("")) {
                // 提取参数后需要再次unescape
                true_refer = unescape(true_refer);
                int queryIndex = true_refer.indexOf('?');
                if (queryIndex != -1 && queryIndex < (true_refer.length() - 1)) {
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

    /**
     * 组装建树前对日志分组的key
     * 
     * @param message
     *            PB格式的message对象
     * @param groupingFields
     *            从xml配置文件解析出的用于分组的字段名称
     * @return
     */
    public static String wrapGroupingKey(GeneratedMessage message, List<String> groupingFields) {
        boolean isFirst = true;
        StringBuilder sb = new StringBuilder();
        for (String field : groupingFields) {
            FieldDescriptor fd = message.getDescriptorForType().findFieldByName(field);
            if (fd != null) {
                if (!isFirst) {
                    sb.append("_");
                } else {
                    isFirst = false;
                }
                sb.append(message.getField(fd).toString());
            }
        }
        return sb.toString();
    }
}
