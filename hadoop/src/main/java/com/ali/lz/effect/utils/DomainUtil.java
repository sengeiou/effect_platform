package com.ali.lz.effect.utils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.net.InternetDomainName;

public class DomainUtil {

    public static final Pattern shopTaobaoDomainPattern = Pattern.compile("shop[0-9]+.taobao.com", Pattern.MULTILINE);
    public static Map<String, DomainName> domainNameCache = new HashMap<String, DomainName>();
    public static final Pattern extractDomainPattern = Pattern.compile("^(([^:/?#]+):)?(//([^/?#]*))?");

    static class DomainName {
        String domainNameLevel1;
        String domainNameLevel2;

        public DomainName(String domainNameLevel1, String domainNameLevel2) {
            this.domainNameLevel1 = domainNameLevel1;
            this.domainNameLevel2 = domainNameLevel2;
        }
    }

    /**
     * Extract domain name from url using Guava InternetDomainName. <br>
     * <b>注意</b>：此方法对输入的url有如下要求：url必须已经过trim和decode处理. <br>
     * 
     * @param url
     * @param domainLevel
     *            域名级别, 1表示一级域名, 2表示二级域名, 其他值表示全域名
     * 
     * @return 当url为null或违反RFC 2396规范时, 返回"". <br>
     *         当domainLevel为1时返回一级域名, <br>
     *         当domainLevel为2时返回二级域名,
     *         对于类似shop1234.taobao.com的店铺域名，模糊化后统一返回"shopXX.taobao.com", <br>
     *         其他情况返回完整域名.
     * 
     * @author feiqiong.dpf
     */
    public static String getDomainFromUrlWithCache(String url, int domainLevel) {
        String domainName = DomainUtil.parseDomainName(url);
        try {
            DomainUtil.DomainName cachedDomainName = DomainUtil.domainNameCache.get(domainName);
            String domainNameLevel1 = "";
            String domainNameLevel2 = "";
            if (cachedDomainName != null) {
                domainNameLevel1 = cachedDomainName.domainNameLevel1;
                domainNameLevel2 = cachedDomainName.domainNameLevel2;
            } else {
                InternetDomainName internetDomainName = InternetDomainName.from(domainName);
                if (internetDomainName.hasPublicSuffix()) {
                    domainNameLevel1 = internetDomainName.topPrivateDomain().name();
                    InternetDomainName parentDomainName = internetDomainName.parent();
                    while (parentDomainName.hasParent()) {
                        if (parentDomainName.name().equals(domainNameLevel1)) {
                            domainNameLevel2 = internetDomainName.name();
                        }
                        internetDomainName = parentDomainName;
                        parentDomainName = parentDomainName.parent();
                    }
                    if (shopTaobaoDomainPattern.matcher(domainNameLevel2).find()) {
                        domainNameLevel2 = "shopXX.taobao.com";
                    }
                    DomainUtil.domainNameCache.put(domainName, new DomainUtil.DomainName(domainNameLevel1,
                            domainNameLevel2));
                }
            }
            if (domainLevel == 1) {
                return domainNameLevel1;
            } else if (domainLevel == 2) {
                return domainNameLevel2;
            }

        } catch (IllegalArgumentException e) {
            return domainName;
        }

        return domainName;
    }

    @Deprecated
    public static String getDomainFromUrlExt(String url, int domainLevel) {
        String domainName = DomainUtil.parseDomainName(url);
        if (domainLevel != 1 && domainLevel != 2)
            return domainName;
        try {
            InternetDomainName internetDomainName = InternetDomainName.from(domainName);
            if (internetDomainName.hasPublicSuffix()) {
                String domainNameLevel1 = internetDomainName.topPrivateDomain().name();
                if (domainLevel == 1)
                    return domainNameLevel1;
                else {
                    InternetDomainName parentDomainName = internetDomainName.parent();
                    String domainNameLevel2 = "";
                    while (parentDomainName.hasParent()) {
                        if (parentDomainName.name().equals(domainNameLevel1)) {
                            domainNameLevel2 = internetDomainName.name();
                        }
                        internetDomainName = parentDomainName;
                        parentDomainName = parentDomainName.parent();
                    }
                    if (shopTaobaoDomainPattern.matcher(domainNameLevel2).find()) {
                        domainNameLevel2 = "shopXX.taobao.com";
                    }
                    return domainNameLevel2;
                }
            }
        } catch (IllegalArgumentException e) {
            return domainName;
        }

        return domainName;
    }

    /**
     * 利用正则从url中提取域名, 比直接使用JDK中的URI.getHost()平均要快2.5倍. <br>
     * 但对特殊url可能存在提取域名失败的情况, 需要注意！
     * 
     * @param url
     * @return domain name
     */
    public static String getDomainNameByRegex(String url) {
        String domain = "";
        Matcher matcher = DomainUtil.extractDomainPattern.matcher(url);
        if (matcher.find()) {
            domain = matcher.group(matcher.groupCount());
        }
        return domain;
    }

    /**
     * 利用JDK URI方式获取域名, 如果对性能无严格要求, 建议使用
     * 
     * @param url
     * @return
     */
    public static String getHostName(String url) {
        String domain = null;
        URI uri;
        try {
            uri = new URI(url);
            domain = uri.getHost();
        } catch (URISyntaxException e) {
            domain = "";
        } catch (NullPointerException e) {
            domain = "";
        }
        return domain;
    }

    /**
     * 直接提取url中:// 和 /之间的字符串. <b>效率最高</b>
     * 
     * @param url
     * @return domain name
     * 
     * @author feiqiong.dpf
     */
    public static String parseDomainName(String url) {
        if (url == null)
            return "";
        int begin = url.indexOf("://");
        if (begin == -1)
            begin = 0;
        else
            begin += 3;
        int end = url.indexOf("/", begin);
        if (end != -1)
            return url.substring(begin, end);
        else
            return url.substring(begin);
    }

    /**
     * 提取1级或2级域名 注意：该方法从taobao hive
     * udf中的getDomainFromUrl重构得到，当前仅适用于淘内日志的url或refer提取域名，用于站外其他日志时，无法保证能正常提取域名
     * 
     * @param url
     * @param domainLevel
     * @return
     */
    @Deprecated
    public static String getDomainFromUrl(String url, int domainLevel) {
        // level 1表示取一级域名 2表示取二级域名
        if (url == null) {
            return null;
        }

        String sourceUrl = url.toString();
        // http://aaa.bbb.taobao.com/aaa.html?a=2&b=4
        if (sourceUrl.startsWith("http://")) {
            sourceUrl = sourceUrl.substring(7);
        } else if (sourceUrl.startsWith("http%3A//")) {
            sourceUrl = sourceUrl.substring(9);
        } else if (sourceUrl.startsWith("https://")) {
            sourceUrl = sourceUrl.substring(8);
        } else if (sourceUrl.startsWith("https%3A//")) {
            sourceUrl = sourceUrl.substring(10);
        }

        // aaa.bbb.taobao.com/aaa.html?a=2&b=4
        int i = sourceUrl.indexOf("/");
        if (i < 0) {
            i = sourceUrl.length();
        }
        // aaa.bbb.taobao.com
        sourceUrl = sourceUrl.substring(0, i);
        String domain = "";
        String suffix = "";
        if (sourceUrl.endsWith(".com")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 4);
            suffix = ".com";
        } else if (sourceUrl.endsWith(".tel")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 4);
            suffix = ".tel";
        } else if (sourceUrl.endsWith(".tw")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 3);
            suffix = ".tw";
        } else if (sourceUrl.endsWith(".kr")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 3);
            suffix = ".kr";
        } else if (sourceUrl.endsWith(".mobi")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 5);
            suffix = ".mobi";
        } else if (sourceUrl.endsWith(".net")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 4);
            suffix = ".net";
        } else if (sourceUrl.endsWith(".org")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 4);
            suffix = ".org";
        } else if (sourceUrl.endsWith(".asia")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 5);
            suffix = ".asia";
        } else if (sourceUrl.endsWith(".me")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 3);
            suffix = ".me";
        } else if (sourceUrl.endsWith(".com.cn")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 7);
            suffix = ".com.cn";
        } else if (sourceUrl.endsWith(".net.cn")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 7);
            suffix = ".net.cn";
        } else if (sourceUrl.endsWith(".org.cn")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 7);
            suffix = ".org.cn";
        } else if (sourceUrl.endsWith(".gov.cn")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 7);
            suffix = ".gov.cn";
        } else if (sourceUrl.endsWith(".hk")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 3);
            suffix = ".hk";
        } else if (sourceUrl.endsWith(".tv")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 3);
            suffix = ".tv";
        } else if (sourceUrl.endsWith(".biz")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 4);
            suffix = ".biz";
        } else if (sourceUrl.endsWith(".cc")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 3);
            suffix = ".cc";
        } else if (sourceUrl.endsWith(".name")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 5);
            suffix = ".name";
        } else if (sourceUrl.endsWith(".info")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 5);
            suffix = ".info";
        } else if (sourceUrl.endsWith(".cn")) {
            domain = sourceUrl.substring(0, sourceUrl.length() - 3);
            suffix = ".cn";
        }

        // yuming的值 = aaa.bbb.taobao
        String[] strList = domain.split("\\.");

        String returnResult = "";
        if (domainLevel == 1) {
            String domainNameLevel1 = strList[strList.length - 1];
            domainNameLevel1 = domainNameLevel1 + suffix;
            returnResult = domainNameLevel1;
        } else if (domainLevel == 2) {
            if (strList.length == 1) {
                domain = strList[0];
            } else if (strList.length == 2) {
                domain = strList[0] + "." + strList[1];
            } else if (strList.length > 2) {
                domain = strList[strList.length - 2] + "." + strList[strList.length - 1];
            }
            // 最后得到2级域名
            String domainNameLevel2 = domain + suffix;

            if (shopTaobaoDomainPattern.matcher(domainNameLevel2).find()) {
                domainNameLevel2 = "shopXX.taobao.com";
            }
            returnResult = domainNameLevel2;
        }

        return returnResult;
    }
}
