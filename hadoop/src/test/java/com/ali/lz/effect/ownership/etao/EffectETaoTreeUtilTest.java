package com.ali.lz.effect.ownership.etao;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.regex.Pattern;

import junit.framework.TestCase;

import org.junit.Before;
import org.junit.Test;

import com.ali.lz.effect.holotree.HoloTreeUtil;
import com.ali.lz.effect.ownership.etao.ETaoSourceType;
import com.ali.lz.effect.ownership.etao.EffectETaoTreeUtil;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;

public class EffectETaoTreeUtilTest extends TestCase {

    @Before
    public void setUp() {
    }

    @Test
    public void test1() {
        ETaoSourceType sourceType = EffectETaoTreeUtil.parseSrc("http://buy.etao.com/?",
                "http://www.baidu.com/s?wd=etao&rsv_spt=1&issp=1&rsv_bp=0&ie=utf-8&tn=baiduhome_pg&inputT=2471",
                ETaoSourceType.LP_SRC_TYPE);
        assertEquals(sourceType.getSrc_id(), ETaoSourceType.SEO);
        String url = "http://vip.etao.com/?tb_lm_id=t_nvlang_id=juexiao&pid=mm_0_0_0&upsid=1355652459_3t2_1254544228";
        sourceType = EffectETaoTreeUtil.parseSrc(url, "", ETaoSourceType.LP_SRC_TYPE);
        assertEquals(sourceType.getSrc_id(), ETaoSourceType.TB_LM_ID);

        String url1 = "http://jf.etao.com/?t?spm=1.310059.279451.6.rWObSZ&tb_lm_id=w_m16";
        sourceType = EffectETaoTreeUtil.parseSrc(url1, "", ETaoSourceType.LP_SRC_TYPE);
        assertEquals(sourceType.getSrc_id(), ETaoSourceType.TB_LM_ID);
    }

    @Test
    public void test2() {
        Pattern pattern = Pattern.compile("^(http|https)://s\\.etao\\.com.*");
        String url = "http://pass.etao.com/add?t=679ca4e846113d7bb1fcced33ebfe8cd&ck1=UUkM9AAPZOWLA+k=&tracknick=ellababy110&cookie2=40d2d8b01b972a0a7b15b486bfd336ff&uc1=cookie15=U+GCWk/75gdr5Q==&unb=212372001&_nk_=ellababy110&_l_g_=Ug==&cookie1=W8CP0PtGFYw7RVCfvZel8Gq+db2HTln0ALUxSnmCXkQ=&cookie17=UUkM9AAPZOWL&_tb_token_=GmwuVvGmSl&target=http://s.etao.com/search?spm=a230r.1.0.674.aba636&v=auction&q=%CD%ED%C0%F1%B7%FE+%D0%C2%BF%EE2012&nidpromote=865131947464691976:3&stp=tb_search.srp.opsrh4.0.etaosrp&tb_lm_id=t_sousuo7&cat=50103022&tbpm=t&pacc=ScN06QG_m13EvJm5KeTLTQ==&opi=61.50.218.130&tmsc=1348045521";
        assertFalse(pattern.matcher(url).find());

        String etaourl = "http://i.daily.etao.net/message/list.html?cate=jfb&fl=1356337843";
        assertFalse(Constants.etaoPattern.matcher(etaourl).find());
    }

    @Test
    public void test3() {
        String trade_track_info = EffectETaoTreeUtil
                .parseTradeTrackInfo(
                        "http://detail.etao.com/detail.htm?url=http%3A%2F%2Fitem.hecha.cn%2Fsgoods-2521.html&rebatepartner=183&trade_track_info=370014",
                        "", "");
        assertEquals(trade_track_info, "370014");
    }

    @Test
    public void test4() {
        String url = "http://shop.etao.com/redirect.htm?nid=15522812939&sid=1&target=http://s.click.taobao.com/t?e=zGU34CA7K%2BPkqB07S4%2FK0CFcRfH0El%2BpZcoXo5QWgwUXfXdKis64TgAFzMGI4UyiqoHGiWeXW8RSBluEmEbhSuTTfUMqsm8Z3f7o044zThtFDW9E&pid=mm_14507426_0_0&trade_track_info=jcy_15522812939&sign=f493ccb618c58b40189746f93a5f175a";
        String refer = "http://s.click.taobao.com/t_js?tu=http%3A%2F%2Fs.click.taobao.com%2Ft%3Fe%3DzGU34CA7K%252BPkqB07S4%252FK0CFcRfH0El%252BpZcoXo5QWgwUXfXdKis64TgAFzMGI4UyiqoHGiWeXW8RSBluEmEbhSuTTfUMqsm8Z3f7o044zThtFDW9E%26pid%3Dmm_14507426_0_0%26ref%3Dhttp%253A%252F%252Fshop.etao.com%252Fredirect.htm%253Fnid%253D15522812939%2526sid%253D1%2526target%253Dhttp%25253A%25252F%25252Fs.click.taobao.com%25252Ft%25253Fe%25253DzGU34CA7K%2525252BPkqB07S4%2525252FK0CFcRfH0El%2525252BpZcoXo5QWgwUXfXdKis64TgAFzMGI4UyiqoHGiWeXW8RSBluEmEbhSuTTfUMqsm8Z3f7o044zThtFDW9E%252526pid%25253Dmm_14507426_0_0%2526trade_track_info%253Djcy_15522812939%2526sign%253Df493ccb618c58b40189746f93a5f175a%26et%3DjFBAg7xXCuSBvQ%253D%253D";
        String fixedUrl = HoloTreeUtil.unescape(HoloTreeUtil.transformUrl(url));

        System.out.println(fixedUrl);
        String fixedRefer = HoloTreeUtil.getTrueReferer(refer);
        System.out.println(fixedRefer);
        assertEquals(fixedUrl, fixedRefer);
    }

    @Test
    public void test5() throws UnsupportedEncodingException {
        // String url = "";
        // String fixedUrl =
        // HoloTreeUtil.unescape(HoloTreeUtil.transformUrl(url));
        // System.out.println(fixedUrl);
        // String refer =
        // "http://s.click.taobao.com/t_js?tu=http%3A%2F%2Fs.click.taobao.com%2Ft%3Fe%3DzGU34CA7K%252BPkqB07S4%252FK0CFcRfH0HjRlO4Y1eL38rCs5Ore1x6TKTnQSltpm1Z6TKEbuZPFqPJ1f%252BqHt27HgHPindRguNusko2Smd7tQNxtjcJQpwDapOAtQPmEICc5JdKhSUrVTU3kFz1yUP5cFw1NqbiAQCZUxqpxXAgp4xkQHxxdIrQcYwWgymf1RmFg7mpPsw3aCkFmNGRLk%252FM9zC3jazcPcgbWdbFkw4KrBXlmaUB%252FH%26pid%3Dmm_10011550_2325296_9002527%26unid%3D0%26ref%3Dhttp%253A%252F%252Fs.taobao.com%252Fsearch%253Fq%253D%2525C0%2525CF%2525C4%2525EA%2525BB%2525FA%252B%2525CA%2525D6%2525BB%2525FA%252B%2525D5%2525FD%2525C6%2525B7%2526ex_q%253D%2526filterFineness%253D%2526atype%253D%2526fs%253D1%2526isprepay%253D1%2526promoted_service4%253D4%2526user_type%253D1%2526commend%253Dall%2526source%253Dsuggest%2526ssid%253Ds5-e-p1%2526suggest%253D0_1%2526pid%253Dmm_10011550_2325296_9002527%2526unid%253D0%2526mode%253D63%2526initiative_id%253Dtbindexz_20121007%26et%3DjFBAgoS2fqxLBQ%253D%253D";
        // String refer =
        // "http://s.click.taobao.com/t_js?tu=http%3A%2F%2Fs.click.taobao.com%2Ft_8%3Fspm%3D3.39305.253560.277%26e%3D7HZ5x%252BOzd%252BsVya2vr4TpWlE%252BS0g%253D%26p%3Dmm_10011550_0_0%26ref%3Dhttp%253A%252F%252Fwww.tmall.com%252Fgo%252Fchn%252Ftbk_channel%252Ftmall_new.php%26et%3DjFBB2C2jpwdvBA%253D%253D";
//        String refer = "http://i.click.taobao.com/t_js?tu=http%3A%2F%2Fi.click.taobao.com%2Ft%3Fe%3DzGU34CA7K%252BPkqB07S4%252FK0CFcRfH0G6UGNNN2vkXXh2y2X4QIVxM2idwOe%252B7j5tDeK0jps8U5deGuuJ4DzzRaV0Wpo%252F7kSa3TG2TR1%252FTb5V9RDGhAILXfcUufmPhYw%252BTDbCc1u%252BZC%252Bp99XoM%253D%26spm%3Da2116.1109613.hantong.9.behC3Y%26ref%3Dhttp%253A%252F%252Fshare.uz.taobao.com%252F%253Fspm%253Da2116.3041269.6805133.2.zIwqwR%26et%3DjFBDMT7vZHVlzQ%253D%253D";
        String refer = "http://i.click.taobao.com/t_js?tu=http%3A%2F%2Fi.click.taobao.com%2Ft%3Fe%3DzGU34CA7K%252BPkqB07S4%252FK0CFcRfH0G6UGNNN2vkXXh2y2X4QIVxM2j8xfvCwyYGbXwRlIlNxRMPgi1814iTlerwSLnCrRhmqdaS5SbY4XB6OJIu7n%252BJpHkMbbGBTQ4OH3JeSw9LNSVsrQZ8c%253D%26spm%3Da2116.1109613.hantong.4.CCPOG0%26ref%3Dhttp%253A%252F%252Fshare.uz.taobao.com%252Fdetail.htm%253Fspm%253Da2116.1109613.w9006-179910834.1.behC3Y%2526type%253D2%2526contentId%253D17738027432533886%2526id%253D1555267%2526isDetail%253D1%26et%3DjFBDMT7vZHNWng%253D%253D"; 
        String fixedRefer = HoloTreeUtil.getTrueReferer(refer);
//        int pos = refer.indexOf('?');
//        if (pos != -1) {
//            String queryString = refer.substring(pos + 1);
//            queryString = HoloTreeUtil.unescape(HoloTreeUtil.transformUrl(queryString.split("=")[1]));
//            String fixedRefer = URLDecoder.decode(queryString, "utf-8");
//
            System.out.println(fixedRefer);
//        }
        // assertEquals(fixedUrl.replace('+', ' '), fixedRefer);
    }

    @Test
    public void test6() {
        // 测试修复url/refer时对回车换行的处理
        String refer = "http://so.360.cn/s?q=%E6%B5%B7%E8%93%9D%E4%B9%8B%E8%B0%9C+%E7%BE%8E%E7%99%BD%E7%84%95%E5%BD%A9%E4%BA%AE%E5%A6%8D%E7%B2%BE%E5%8D%8E3ml%E7%BE%8E%E7%99%BD%E6%B7%A1%E6%96%91%E7%89%B9%E4%BB%B7%0D%0A%E4%B8%BE%E6%8A%A5%E4%B8%AD%E5%BF%83%0D%0A+%0D%0A%0D%0A%E5%96%9C%E6%AC%A2(1)%0D%0A+%E6%94%B6%E8%97%8F%E5%AE%9D%E8%B4%9D(19)%0D%0A%E4%BB%B7%E3%80%80%E3%80%80%E6%A0%BC%EF%BC%9A77.00%E5%85%83%0D%0A%E7%89%A9%E6%B5%81%E8%BF%90%E8%B4%B9%EF%BC%9A%0D%0A%E5%8C%97%E4%BA%AC%7C%E8%87%B3+%E5%8C%97%E4%BA%AC%E5%BF%AB%E9%80%92%3A6.00%E5%85%83+EMS%3A26.00%E5%85%83+%E5%B9%B3%E9%82%AE%3A20.00%E5%85%83%0D%0A3&src=360chrome_addr&ie=utf-8";
        // String refer =
        // "http://www.duomeishop.com/article-377.html?page%0A%0A-helpcenter.html";
        String fixedRefer = HoloTreeUtil.unescape(refer);
        System.out.println(fixedRefer);
        assertEquals(fixedRefer, HoloTreeUtil.unescape(HoloTreeUtil.transformUrl(refer)));
    }

    @Test
    public void test7() {
        String url = "http://tao.etao.com/auction?keyword=%B1%CA%BC%C7%B1%BE%B5%E7%C4%D4&catid=1101&refpid=mm_10011550_2325296_9002528&digest=84663B50525EEE11CAC71AD42B3427ED&crtid=142751242&itemid=5573558266&adgrid=134726720&eurl=http%3A%2F%2Fclick.simba.taobao.com%2Fcc_im%3Fp%3D%26s%3D1634227824%26k%3D288%26e%3DUKryNKlTl%252FFvVeRLK52Nmxx3v6X9nXRV8oGxwGwbsQb30orkAtf4t5DiTZ3XqpLwxXmr1BW8v9bl92BQD0YTfYbx0W9FXvZf%252B9Yi1KBSXf20Muul1%252F4F6W38lajAyqoCZpGSwIwek%252B7YsXs6Bn2X1cVCjKpKqR1SzSo2RWqxHWt%252BTqHUKHQ8C%252BmuSHud0wz9ji3THGhgQImJiXaVrVVoVEtvpSHePuQHkd12XRdPz7ot6rNxE79b0sYU%252FzGWtwC%252F3lMycUkFVD%252FU4HuW6GqQVCvNpRUUK%252Bwr&refpos=295_66172_21&clk1=34f9415389b5f6d0eaecce64f8d8f8fb";
        String true_url = HoloTreeUtil.transformUrl(url);
        System.out.println(StringUtil.getUrlParameter(true_url, "eurl"));
    }

}
