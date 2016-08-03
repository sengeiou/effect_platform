package com.ali.lz.effect.utils;

import java.net.URLDecoder;
import java.security.MessageDigest;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.CRC32;

import org.apache.hadoop.hbase.util.Bytes;

public class StringUtil {
    public static int parseInt(String s, int defaultInt) {
        int result = defaultInt;

        if (!s.isEmpty()) {
            try {
                result = Integer.parseInt(s);
            } catch (NumberFormatException ne) {
            }
        }

        return result;
    }

    public static byte[] md5(MessageDigest md5Instance, String str) {
        byte[] strBytes = str.getBytes();
        byte[] md5Bytes = md5Instance.digest(strBytes);
        return md5Bytes;
    }

    // 把key-value格式的字符串解析成map
    public static Map<String, String> splitStr(String str, String firstLevelSeparator, String secondLevelSeparator) {
        Map<String, String> strMap = new HashMap<String, String>();

        String[] strArray = str.split(firstLevelSeparator);

        int size = strArray.length;
        for (int i = 0; i < size; i++) {
            String[] currentPairArray = strArray[i].split(secondLevelSeparator, -1);

            if (currentPairArray.length >= 2) {
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
                Map<String, String> kvPairs = StringUtil.splitStr(queryString, "&", "=");
                if (kvPairs.containsKey(key)) {
                    value = URLDecoder.decode(kvPairs.get(key), "UTF-8");
                }
            }
        } catch (Exception e) {
        }
        return value;
    }

    /**
     * 暂时用于解决从站外url中提取搜索关键字乱码的问题 注意：仍有一定概率乱码。尝试ICU4J编码检测工具包效果并不理想
     * 
     * @param url
     * @param key
     * @param recommendDecoding
     * @return
     */
    public static String getUrlKeyword(String url, String key, String recommendDecoding) {
        String value = "";
        String keywordDecoding = recommendDecoding;
        String detectDecoding = keywordDecoding;
        if ((detectDecoding = getUrlParameter(url, "ue")).length() <= 0)
            if ((detectDecoding = getUrlParameter(url, "ie")).length() <= 0)
                if ((detectDecoding = getUrlParameter(url, "_input_charset")).length() <= 0)
                    detectDecoding = keywordDecoding;
        try {
            int pos = url.indexOf('?');
            if (pos != -1) {
                String queryString = url.substring(pos + 1);
                Map<String, String> kvPairs = StringUtil.splitStr(queryString, "&", "=");
                if (kvPairs.containsKey(key)) {
                    value = URLDecoder.decode(kvPairs.get(key), detectDecoding);
                }
            }
        } catch (Exception e) {
        }
        return value;
    }

    public static String join(String[] tokens, String separator) {
        if (tokens == null || tokens.length == 0)
            return "";

        StringBuilder sb = new StringBuilder();
        for (String token : tokens) {
            sb.append(token);
            sb.append(separator);
        }
        sb.setLength(sb.length() - separator.length());
        return sb.toString();
    }

    public static String join(List<String> tokens, String separator) {
        return join((String[]) tokens.toArray(new String[tokens.size()]), separator);
    }

    public static String join(Set<String> tokens, String separator) {
        return join((String[]) tokens.toArray(new String[tokens.size()]), separator);
    }

    public static boolean isNum(String str) {
        int cnt = str == null ? 0 : str.length();
        if (cnt == 0)
            return false;
        for (int i = 0; i < cnt; i++)
            if (!Character.isDigit(str.charAt(i)))
                return false;
        return true;
    }

    public static void debug(String msg) {
        if (Constants.DEBUG == true) {
            System.out.println(msg);
        }
    }

    public static long getTimestamp(String time, String timeFormat) {
        // timeFormat example : "yyyyMMddHHmmss"
        DateFormat format = new SimpleDateFormat(timeFormat);
        java.util.Date date;
        try {
            date = format.parse(time);
        } catch (ParseException e) {
            return 0;
        } catch (NullPointerException e) {
            return 0;
        }
        return date.getTime() / 1000;
    }

    public static String getDateFromTs(long timestamp, String timeFormat) {
        DateFormat format = new SimpleDateFormat(timeFormat);
        Date date = new Date(timestamp);
        return format.format(date);
    }

    /**
     * 截取给定 SPM 的前 4 段内容计算 CRC32 校验和
     * 
     * @param spm
     * @return
     */
    public static int spmCrc32(String spm) {
        int cnt = 0;
        int pos = -1;

        // 自行计算 SPM 串前四段内容结束位置，避免原先用 String.split() 方法的匹配切分开销
        do {
            pos = spm.indexOf('.', pos + 1);
            if (pos != -1)
                cnt++;
        } while (cnt < 4 && pos != -1);

        CRC32 h = new CRC32();
        if (cnt == 4) {
            // 新的五段式 SPM 串，只取前四段内容计算 CRC32
            h.update(Bytes.toBytes(spm.substring(0, pos)));
        } else if (cnt == 3) {
            // 老的四段式 SPM 串，全部内容都参与 CRC32 计算
            h.update(Bytes.toBytes(spm));
        } else {
            // 无效的 SPM 串
            return 0;
        }

        return (int) h.getValue();
    }

    public static void main(String[] args) {
        System.out.println(StringUtil.getTimestamp("234aa23", "yyyyMMdd"));
        System.out.println(StringUtil.getTimestamp(null, "yyyyMMdd"));
        System.out.println(StringUtil.getTimestamp("20221219", "yyyyMMdd"));
        System.out.println(StringUtil.getTimestamp("20121216235959", "yyyyMMddHHmmss"));
        System.out.println(StringUtil.getTimestamp("20121217000002", "yyyyMMddHHmmss"));
        System.out.println(DomainUtil.getDomainFromUrl("http://vip.etao.com/123131", 2));
        System.out.println(StringUtil.getDateFromTs(1355673599, "yyyy-MM-dd HH:mm:ss"));

    }

}
