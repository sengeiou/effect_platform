package com.ali.lz.effect.tools.data2mysql;

import java.security.MessageDigest;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

public class MD5Digest {

    public MD5Digest() {
        // TODO Auto-generated constructor stub
    }

    /**
     * 利用MD5进行加密
     * 
     * @param srcStr
     *            待加密的字符串
     * @return 加密后的十六进制字符串
     */
    public static String encode(String srcStr) {
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            if (srcStr == null)
                return "";
            byte[] srcBytes = srcStr.getBytes("utf-8");
            byte[] md5Bytes = md5.digest(srcBytes);
            StringBuffer hexValue = new StringBuffer(32);
            for (int i = 0; i < md5Bytes.length; i++) {
                int val = ((int) md5Bytes[i]) & 0xff;
                if (val < 16) {
                    hexValue.append("0");
                }
                hexValue.append(Integer.toHexString(val));
            }
            return hexValue.toString();
        } catch (NoSuchAlgorithmException e) {
            return "";
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }

    /**
     * 判断字符串是否存在MD5加密值
     * 
     * @param srcStr
     *            待查找的字符串
     * @param md5Str
     *            加密后的MD5值
     * @return 是否匹配成功
     */
    public static boolean checkExist(String srcStr, String md5Str) {
        if (encode(srcStr).equals(md5Str))
            return true;
        else
            return false;
    }

}
