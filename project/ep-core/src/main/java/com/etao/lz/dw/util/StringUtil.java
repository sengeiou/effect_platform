package com.etao.lz.dw.util;

import java.net.URLDecoder;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class StringUtil {
	
	public static String getDomainFromUrl(String url, int domainLevel) {
		//level  1表示取一级域名  2表示取二级域名
		if (url == null) {
		      return null;
		    }

		    String sourceUrl = url.toString();
		       //http://aaa.bbb.taobao.com/aaa.html?a=2&b=4
	        if(sourceUrl.startsWith("http://")){
	     	   sourceUrl = sourceUrl.substring(7);
	        }

	         if(sourceUrl.startsWith("http%3A//")){
	      	   sourceUrl = sourceUrl.substring(9);
	         }

		       //aaa.bbb.taobao.com/aaa.html?a=2&b=4
		       int i = sourceUrl.indexOf("/");
		       if(i<0){
		    	   i = sourceUrl.length();
		       }
		       //aaa.bbb.taobao.com
		       sourceUrl = sourceUrl.substring(0,i);
		       String yuming = "";
		       String houzhui = "";
		       if(sourceUrl.endsWith(".com")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-4);
		    	   houzhui = ".com";
		       }else if(sourceUrl.endsWith(".tel")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-4);
		    	   houzhui = ".tel";
		       }else if(sourceUrl.endsWith(".mobi")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-4);
		    	   houzhui = ".mobi";
		       }else if(sourceUrl.endsWith(".net")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-4);
		    	   houzhui = ".net";
		       }else if(sourceUrl.endsWith(".org")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-4);
		    	   houzhui = ".org";
		       }else if(sourceUrl.endsWith(".asia")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-5);
		    	   houzhui = ".asia";
		       }else if(sourceUrl.endsWith(".me")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-3);
		    	   houzhui = ".me";
		       }else if(sourceUrl.endsWith(".com.cn")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-7);
		    	   houzhui = ".com.cn";
		       }else if(sourceUrl.endsWith(".net.cn")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-7);
		    	   houzhui = ".net.cn";
		       }else if(sourceUrl.endsWith(".org.cn")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-7);
		    	   houzhui = ".org.cn";
		       }else if(sourceUrl.endsWith(".gov.cn")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-7);
		    	   houzhui = ".gov.cn";
		       }else if(sourceUrl.endsWith(".hk")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-3);
		    	   houzhui = ".hk";
		       }else if(sourceUrl.endsWith(".tv")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-3);
		    	   houzhui = ".tv";
		       }else if(sourceUrl.endsWith(".biz")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-4);
		    	   houzhui = ".biz";
		       }else if(sourceUrl.endsWith(".cc")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-3);
		    	   houzhui = ".cc";
		       }else if(sourceUrl.endsWith(".name")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-5);
		    	   houzhui = ".name";
		       }else if(sourceUrl.endsWith(".info")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-5);
		    	   houzhui = ".info";
		       }else if(sourceUrl.endsWith(".cn")){
		    	   yuming = sourceUrl.substring(0, sourceUrl.length()-3);
		    	   houzhui = ".cn";
		       }
		       
		       //yuming的值  = aaa.bbb.taobao
		       String[] strList = yuming.split("\\.");
		       
		       String returnResult = "";
		       if(domainLevel == 1){
			       String yuming1 = strList[strList.length-1];
			       yuming1 = yuming1 + houzhui;
			       returnResult = yuming1;
		       }else if(domainLevel == 2){
			       if(strList.length==1){
			    	   yuming = strList[0];
			       }else if(strList.length==2){
			    	   yuming = strList[0]+"."+strList[1];
			       }else if(strList.length>2){
			    	   yuming = strList[strList.length-2] +"."+ strList[strList.length-1];
			       }
			       
			     //最后得到2级域名
			       String yuming2 = yuming+houzhui;
			       Pattern p = Pattern.compile("shop[0-9]+.taobao.com", Pattern.MULTILINE);
			       if(p.matcher(yuming2).find()){
			    	   yuming2 = "shopXX.taobao.com";
			       }
			       returnResult = yuming2;
		       }

		    return returnResult;
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
				Map<String, String> kvPairs = StringUtil.splitStr(queryString,
						"&", "=");
				if (kvPairs.containsKey(key)) {
					value = URLDecoder.decode(kvPairs.get(key), "utf-8");
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
		return join((String[]) tokens.toArray(new String[tokens.size()]),
				separator);
	}

	public static String join(Set<String> tokens, String separator) {
		return join((String[]) tokens.toArray(new String[tokens.size()]),
				separator);
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

	public static void main(String[] args) {
		System.out.println(StringUtil.getTimestamp("234aa23", "yyyyMMdd"));
		System.out.println(StringUtil.getTimestamp(null, "yyyyMMdd"));
		System.out.println(StringUtil.getDomainFromUrl("http://vip.etao.com/123131", 2));

	}

}
