package com.ali.lz.effect.utils;

import org.junit.Test;

import com.ali.lz.effect.utils.DomainUtil;
import com.google.common.base.Stopwatch;

public class DomainUtilTest {

    @Test
    public void testGetHostName() {
        String[] urls = new String[] {
                "http://www.google.com.tw/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&ved=0CDEQFjAA&url=http://www.etao.com/&ei=IzHNUPjTNfDymAWayoDwCA&usg=AFQjCNEsVHBrjdLF1C_ggH-uWxGV-YrOwQ&sig2=KW4d90KKwsE-Z2Pi0RXnLQ&bvm=bv.1355325884,d.dGY",
                "https://jf.alipay.com/prod/pay.htm",
                "http://item.taobao.com/item.htm?id=15893545263&ali_trackid=2:mm_26866744_2384196_9192992:1355999335_4k1_797013116",
                "http://item.taobao.com/item.htm?id=1589354526116", "http://www.daily.etao.net/",
                "http://127.0.0.1:8080/index.html", "http://www.189.sh/liantong.html",
                "http://www.amita.pygzs.com/viewproduct.asp?id=153",
                "http://uy3gp.com/Nigarim_Kino_Tur.asp?djcat_id=5",
                "http://real.wangpiao.com/cms/MoviesWorld/starinfo/StarInfoDiscription.aspx?StarID=270" };
        for (String url : urls) {
            System.out.println(DomainUtil.getHostName(url));
            System.out.println(DomainUtil.getDomainNameByRegex(url));
            System.out.println(DomainUtil.parseDomainName(url));
        }

        int loopCounter = 10000;
        Stopwatch stopwatch = new Stopwatch().start();
        for (int i = 0; i < loopCounter; i++) {
            for (String url : urls)
                DomainUtil.getHostName(url);
        }
        stopwatch.stop();
        System.out.println("getHostName : " + stopwatch.toString());

        stopwatch.reset();
        stopwatch.start();
        for (int i = 0; i < loopCounter; i++) {
            for (String url : urls)
                DomainUtil.getDomainNameByRegex(url);
        }
        stopwatch.stop();
        System.out.println("getHostNameByRegex : " + stopwatch.toString());

        stopwatch.reset();
        stopwatch.start();
        for (int i = 0; i < loopCounter; i++) {
            for (String url : urls)
                DomainUtil.parseDomainName(url);
        }
        stopwatch.stop();
        System.out.println("parseDomainName : " + stopwatch.toString());
    }

    @Test
    public void testGetDomainFromUrl() {

        String[] urls = new String[] {
                "http://www.google.com.tw/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&ved=0CDEQFjAA&url=http://www.etao.com/&ei=IzHNUPjTNfDymAWayoDwCA&usg=AFQjCNEsVHBrjdLF1C_ggH-uWxGV-YrOwQ&sig2=KW4d90KKwsE-Z2Pi0RXnLQ&bvm=bv.1355325884,d.dGY",
                "https://jf.alipay.com/prod/pay.htm",
                "http://item.taobao.com/item.htm?id=15893545263&ali_trackid=2:mm_26866744_2384196_9192992:1355999335_4k1_797013116",
                "http://item.taobao.com/item.htm?id=1589354526116", "http://www.daily.etao.net/",
                "http://127.0.0.1:8080/index.html", "http://www.189.sh/liantong.html",
                "http://www.amita.pygzs.com/viewproduct.asp?id=153",
                "http://uy3gp.com/Nigarim_Kino_Tur.asp?djcat_id=5",
                "http://real.wangpiao.com/cms/MoviesWorld/starinfo/StarInfoDiscription.aspx?StarID=270" };

        for (String url : urls) {
            System.out.println("getDomainFromUrl    domain level1 : " + DomainUtil.getDomainFromUrl(url, 1));
            System.out.println("getDomainFromUrlExt domain level1 : " + DomainUtil.getDomainFromUrlWithCache(url, 1));
            System.out.println("getDomainFromUrl    domain level2 : " + DomainUtil.getDomainFromUrl(url, 2));
            System.out.println("getDomainFromUrlExt domain level2 : " + DomainUtil.getDomainFromUrlWithCache(url, 2));
        }

        int loopCounter = 10000;
        Stopwatch stopwatch = new Stopwatch().start();
        for (int i = 0; i < loopCounter; i++) {
            for (String url : urls) {
                DomainUtil.getDomainFromUrl(url, 1);
                DomainUtil.getDomainFromUrl(url, 2);
            }
        }
        stopwatch.stop();
        System.out.println("depracated getDomainFromUrl: " + stopwatch.toString());

        // test New getDomainFromUrl method
        stopwatch.reset();
        // List<String> domainNames = new ArrayList<String>();
        // for (String url : urls) {
        // domainNames.add(StringUtil.getHostName(url));
        // }
        stopwatch.start();
        for (int i = 0; i < loopCounter; i++) {
            for (String url : urls) {
                DomainUtil.getDomainFromUrlExt(url, 1);
                DomainUtil.getDomainFromUrlExt(url, 2);
            }
        }
        stopwatch.stop();
        System.out.println("guava based getDomainFromUrl: " + stopwatch.toString());
    }

}
