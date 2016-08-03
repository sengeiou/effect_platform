package com.ali.lz.effect.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ali.lz.effect.utils.StringUtil;
import com.google.common.base.Stopwatch;

public class StringUtilTest {

    @Before
    public void setUp() throws Exception {
    }

    @Test
    public void testGetUrlParameter() {
        // String url1 =
        // "%u98CE%u5C1A%u7537%u88C5_%u592A%u5E73%u9E1F%u5B98%u65B9%u5546%u57CEPB89.COM";
        // System.out.println(HoloTreeUtil.unescape(url1));
        String url1 = "http://www.daily.etao.net/";
        System.out.println(Pattern.quote(url1));
    }

    // 测试编码自动检测工具对关键字乱码的处理情况
    @Test
    public void testGetUrlKeyword() {
        String url = "http://tao.etao.com/auction?keyword=%B1%CA%BC%C7%B1%BE%B5%E7%C4%D4&catid=1101&refpid=mm_10011550_2325296_9002528&digest=84663B50525EEE11CAC71AD42B3427ED&crtid=142751242&itemid=5573558266&adgrid=134726720&eurl=http%3A%2F%2Fclick.simba.taobao.com%2Fcc_im%3Fp%3D%26s%3D1634227824%26k%3D288%26e%3DUKryNKlTl%252FFvVeRLK52Nmxx3v6X9nXRV8oGxwGwbsQb30orkAtf4t5DiTZ3XqpLwxXmr1BW8v9bl92BQD0YTfYbx0W9FXvZf%252B9Yi1KBSXf20Muul1%252F4F6W38lajAyqoCZpGSwIwek%252B7YsXs6Bn2X1cVCjKpKqR1SzSo2RWqxHWt%252BTqHUKHQ8C%252BmuSHud0wz9ji3THGhgQImJiXaVrVVoVEtvpSHePuQHkd12XRdPz7ot6rNxE79b0sYU%252FzGWtwC%252F3lMycUkFVD%252FU4HuW6GqQVCvNpRUUK%252Bwr&refpos=295_66172_21&clk1=34f9415389b5f6d0eaecce64f8d8f8fb";
        String refer = "http://so.360.cn/s?q=%E6%B5%B7%E8%93%9D%E4%B9%8B%E8%B0%9C+%E7%BE%8E%E7%99%BD%E7%84%95%E5%BD%A9%E4%BA%AE%E5%A6%8D%E7%B2%BE%E5%8D%8E3ml%E7%BE%8E%E7%99%BD%E6%B7%A1%E6%96%91%E7%89%B9%E4%BB%B7%0D%0A%E4%B8%BE%E6%8A%A5%E4%B8%AD%E5%BF%83%0D%0A+%0D%0A%0D%0A%E5%96%9C%E6%AC%A2(1)%0D%0A+%E6%94%B6%E8%97%8F%E5%AE%9D%E8%B4%9D(19)%0D%0A%E4%BB%B7%E3%80%80%E3%80%80%E6%A0%BC%EF%BC%9A77.00%E5%85%83%0D%0A%E7%89%A9%E6%B5%81%E8%BF%90%E8%B4%B9%EF%BC%9A%0D%0A%E5%8C%97%E4%BA%AC%7C%E8%87%B3+%E5%8C%97%E4%BA%AC%E5%BF%AB%E9%80%92%3A6.00%E5%85%83+EMS%3A26.00%E5%85%83+%E5%B9%B3%E9%82%AE%3A20.00%E5%85%83%0D%0A3&src=360chrome_addr&ie=utf-8";
        System.out.println(StringUtil.getUrlKeyword(url, "keyword", "GB18030"));
        System.out.println(StringUtil.getUrlKeyword(refer, "q", "GB18030"));
        String url1 = "http://www.baidu.com/s?tn=site5566&word=%CE%D2%B5%C4%CC%D4%B1%A61212%B6%A9%B5%A5%BF%B4%B2%BB%BC%FB%C1%CB";
        System.out.println(StringUtil.getUrlKeyword(url1, "word", "GB18030"));
        String url2 = "http://www.baidu.com/s?wd=%BA%A3%CC%D4";
        System.out.println(StringUtil.getUrlKeyword(url2, "wd", "GB18030"));
        String url3 = "http://www.baidu.com/s?ie=utf-8&bs=%E5%9D%9A%E6%9E%9C&f=8&rsv_bp=1&wd=%E7%BE%8E%E5%9B%BD%E5%8D%96%E5%9D%9A%E6%9E%9C%E7%9A%84%E7%A7%8D%E7%B1%BB&rsv_sug3=4&rsv_sug4=5613&rsv_sug=0&rsv_sug1=2&inputT=9326";
        System.out.println(StringUtil.getUrlKeyword(url3, "wd", "GB18030"));
        String url4 = "http://www.baidu.com/s?wd=%D4%DA%CC%D4%B1%A6%C9%CF%D7%F6%C9%FA%D2%E2%CF%D6%D4%DA%C4%D1%D7%F6%C2%F0&rsv_bp=0&ch=&tn=06008006_2_pg&bar=&rsv_spt=3&rsv_sug3=9&rsv_sug=0&rsv_sug1=8&rsv_sug4=1578&inputT=21531";
        System.out.println(StringUtil.getUrlKeyword(url4, "wd", "GB18030"));
        String url5 = "http://www.youdao.com/search?q=%D5%FD%C6%B7%C3%AB%C3%AB%B0%FC%B0%FC&ue=gbk&keyfrom=163.index";
        System.out.println(StringUtil.getUrlKeyword(url5, "q", "GB18030"));

        String url6 = "http://tao.etao.com/search?keyword=%C3%DE%CD%CF%D0%AC&frcatid=50008163&refpid=mm_10011550_2325296_9002528&refpos=refposplaceholder&clk1=818765a6b5ee33f3f6b4a22c8a6442eb";
        System.out.println(StringUtil.getUrlKeyword(url6, "keyword", "GB18030"));
        String url7 = "http://tao.etao.com/search?_input_charset=utf-8&frcatid=50016756&keyword=%E8%BF%90%E5%8A%A8%E8%83%8C%E5%BF%83&refpid=mm_10011550_2325296_9002528&refpos=refposplaceholder&clk1=2e8927e79c1a4f5c283a20ca6d5a5e02";
        System.out.println(StringUtil.getUrlKeyword(url7, "keyword", "GB18030"));
        String url8 = "http://s8.taobao.com/search?q=%CE%DE%CF%DF%CA%F3%B1%EA&tab=all&style=grid&promoted_service4=4&olu=yes&sort=coefp&isnew=2&atype=b&pid=mm_10011550_2325296_9002527&&&&unid=&clk1=74786004dfd05ceace607a134a440f47";
        System.out.println(StringUtil.getUrlKeyword(url8, "q", "GB18030"));
        String url9 = "http://list.taobao.com/itemlist/jiadiano.htm?spm=1.1000386.252639.14.AN1q8C&cat=50035196&viewIndex=1&yp4p_page=0&commend=all&style=list&q=%BC%D2%CD%A5%D3%B0%D4%BA&user_type=0#!cat=50035196&user_type=0&as=0&viewIndex=1&yp4p_page=0&commend=all&atype=b&style=list&q=%E5%AE%B6%E5%BA%AD%E5%BD%B1%E9%99%A2&isnew=2&json=on&tid=0";
        System.out.println(StringUtil.getUrlKeyword(url9, "q", "utf-8"));
        String url10 = "http://s.taobao.com/search?spm=1.1000386.220544.1.AN1q8C&q=%BA%F1%CD%E2%CC%D7&refpid=420460_1006&source=tbsy&pdc=true&style=grid";
        System.out.println(StringUtil.getUrlKeyword(url10, "q", "GB18030"));
        String url11 = "http://list.tmall.com/search_product.htm?q=%D3%F0%C8%DE%B7%FE&commend=all&ssid=s5-e&search_type=mall&sourceId=tb.index&initiative_id=tbindexz_20121221";
        // String url11 =
        // "http://list.tmall.com/search_product.htm?spm=a220m.1000858.1000721.1.DEpfPk&from=sn_1_cat&area_code=330100&style=g&sort=s&q=%BF%ED%CB%C9%C3%AB%D2%C2&n=60&s=0&cat=50025135#J_crumbs&ali_trackid=2:mm_10011550_2325296_9002527:1356076136_3k1_1010647732&spm=3.39305.253560.61";
        System.out.println(StringUtil.getUrlKeyword(url11, "q", "GB18030"));
        String url12 = "http://shopsearch.taobao.com/search?q=%C7%D0%B8%E2&commend=all&ssid=s5-e&search_type=shop&sourceId=tb.index&initiative_id=tbindexz_20121221";
        System.out.println(StringUtil.getUrlKeyword(url12, "q", "GB18030"));
        String url13 = "http://s8.taobao.com/search?q=%C6%A4%B4%F8%20%C4%D0&tab=all&style=grid&promoted_service4=4&olu=yes&sort=coefp&isnew=2&atype=b&pid=mm_10011550_2325296_9002528&&&&unid=&clk1=cd39e28b86a0062dfda9cb67b31e91ad";
        System.out.println(StringUtil.getUrlKeyword(url13, "q", "GB18030"));

        String url14 = "http://s8.taobao.com/search?q=%E6%96%B0%E8%A3%85%E7%A4%BC%E7%89%A9&pid=mm_10011550_0_0";
        String keyword = StringUtil.getUrlKeyword(url14, "q", "UTF-8");
        System.out.println(keyword);
    }

    // 用于临时测试Java中正则的匹配效率
    @Ignore
    public void testRegexPerformance() {
        ArrayList<String> urls = new ArrayList<String>();
        urls.add("http://tao.etao.com/auction?keyword=%B1%CA%BC%C7%B1%BE%B5%E7%C4%D4&catid=1101&refpid=mm_10011550_2325296_9002528&digest=84663B50525EEE11CAC71AD42B3427ED&crtid=142751242&itemid=5573558266&adgrid=134726720&eurl=http%3A%2F%2Fclick.simba.taobao.com%2Fcc_im%3Fp%3D%26s%3D1634227824%26k%3D288%26e%3DUKryNKlTl%252FFvVeRLK52Nmxx3v6X9nXRV8oGxwGwbsQb30orkAtf4t5DiTZ3XqpLwxXmr1BW8v9bl92BQD0YTfYbx0W9FXvZf%252B9Yi1KBSXf20Muul1%252F4F6W38lajAyqoCZpGSwIwek%252B7YsXs6Bn2X1cVCjKpKqR1SzSo2RWqxHWt%252BTqHUKHQ8C%252BmuSHud0wz9ji3THGhgQImJiXaVrVVoVEtvpSHePuQHkd12XRdPz7ot6rNxE79b0sYU%252FzGWtwC%252F3lMycUkFVD%252FU4HuW6GqQVCvNpRUUK%252Bwr&refpos=295_66172_21&clk1=34f9415389b5f6d0eaecce64f8d8f8fb");
        urls.add("http://so.360.cn/s?q=%E6%B5%B7%E8%93%9D%E4%B9%8B%E8%B0%9C+%E7%BE%8E%E7%99%BD%E7%84%95%E5%BD%A9%E4%BA%AE%E5%A6%8D%E7%B2%BE%E5%8D%8E3ml%E7%BE%8E%E7%99%BD%E6%B7%A1%E6%96%91%E7%89%B9%E4%BB%B7%0D%0A%E4%B8%BE%E6%8A%A5%E4%B8%AD%E5%BF%83%0D%0A+%0D%0A%0D%0A%E5%96%9C%E6%AC%A2(1)%0D%0A+%E6%94%B6%E8%97%8F%E5%AE%9D%E8%B4%9D(19)%0D%0A%E4%BB%B7%E3%80%80%E3%80%80%E6%A0%BC%EF%BC%9A77.00%E5%85%83%0D%0A%E7%89%A9%E6%B5%81%E8%BF%90%E8%B4%B9%EF%BC%9A%0D%0A%E5%8C%97%E4%BA%AC%7C%E8%87%B3+%E5%8C%97%E4%BA%AC%E5%BF%AB%E9%80%92%3A6.00%E5%85%83+EMS%3A26.00%E5%85%83+%E5%B9%B3%E9%82%AE%3A20.00%E5%85%83%0D%0A3&src=360chrome_addr&ie=utf-8");
        urls.add("http://www.baidu.com/s?tn=site5566&word=%CE%D2%B5%C4%CC%D4%B1%A61212%B6%A9%B5%A5%BF%B4%B2%BB%BC%FB%C1%CB");
        urls.add("http://s8.taobao.com/search?q=%C6%A4%B4%F8%20%C4%D0&tab=all&style=grid&promoted_service4=4&olu=yes&sort=coefp&isnew=2&atype=b&pid=mm_10011550_2325296_9002528&&&&unid=&clk1=cd39e28b86a0062dfda9cb67b31e91ad");
        urls.add("http://www.baidu.com/s?ie=utf-8&bs=%E5%9D%9A%E6%9E%9C&f=8&rsv_bp=1&wd=%E7%BE%8E%E5%9B%BD%E5%8D%96%E5%9D%9A%E6%9E%9C%E7%9A%84%E7%A7%8D%E7%B1%BB&rsv_sug3=4&rsv_sug4=5613&rsv_sug=0&rsv_sug1=2&inputT=9326");
        urls.add("http://www.baidu.com/s?wd=%D4%DA%CC%D4%B1%A6%C9%CF%D7%F6%C9%FA%D2%E2%CF%D6%D4%DA%C4%D1%D7%F6%C2%F0&rsv_bp=0&ch=&tn=06008006_2_pg&bar=&rsv_spt=3&rsv_sug3=9&rsv_sug=0&rsv_sug1=8&rsv_sug4=1578&inputT=21531");

        ArrayList<Pattern> regexs = new ArrayList<Pattern>();
        regexs.add(Pattern.compile(".*act/sale/hui\\.html.*"));
        regexs.add(Pattern.compile(".*lady/sanlian\\.html.*"));
        regexs.add(Pattern.compile(".*marketing/tejia\\.html.*"));

        long start = System.currentTimeMillis();
        int i = 0;
        while (i < 1000) {
            for (String url : urls) {
                for (Pattern pattern : regexs) {
                    pattern.matcher(url).find();
                }
            }
            i++;
        }
        long end = System.currentTimeMillis();
        System.out.println("elapsed time for \\.* regex match: " + (end - start));

        ArrayList<Pattern> urlParams = new ArrayList<Pattern>();
        urlParams.add(Pattern.compile("act/sale/hui\\.html"));
        urlParams.add(Pattern.compile("lady/sanlian\\.html"));
        urlParams.add(Pattern.compile("marketing/tejia\\.html"));
        start = System.currentTimeMillis();
        i = 0;
        while (i < 1000) {
            for (String url : urls) {
                for (Pattern pattern : regexs) {
                    pattern.matcher(url).find();
                }
            }
            i++;
        }
        end = System.currentTimeMillis();
        System.out.println("elapsed time for \\.* string indexof match: " + (end - start));

        ArrayList<Pattern> regexs1 = new ArrayList<Pattern>();
        regexs1.add(Pattern.compile("http://tao\\.etao\\.com/fushi/search\\.htm.*c=50006842"));
        regexs1.add(Pattern.compile("http://s8\\.taobao\\.com/fushi/search\\.htm.*c=50006843"));
        regexs1.add(Pattern.compile("http://www\\.baidu\\.com/fushi/search\\.htm.*c=50006845"));

        start = System.currentTimeMillis();
        i = 0;
        while (i < 1000) {
            for (String url : urls) {
                for (Pattern pattern : regexs1) {
                    pattern.matcher(url).find();
                }
            }
            i++;
        }
        end = System.currentTimeMillis();
        System.out.println("elapsed time for normal regex match: " + (end - start));
    }

}
