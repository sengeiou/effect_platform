package com.ali.lz.effect.holotree;

import static org.junit.Assert.*;

import org.junit.Test;

import com.ali.lz.effect.holotree.HoloTreeUtil;

public class HoloTreeUtilTest {

    @Test
    public void testPageTypeIdToChar() {
        assertEquals((char) 0x101, HoloTreeUtil.pageTypeIdToChar(1));
        assertEquals((char) 0x1200, HoloTreeUtil.pageTypeIdToChar(0x1100));
    }

    @Test
    public void testReplaceRootPath() {
        String orgRootPath = "\u0000\u0001";
        String expRootPath = "\u0000\u0101";
        int typeId = 1;
        assertEquals(expRootPath, HoloTreeUtil.replacePTRootPathLast(orgRootPath, typeId));
    }

    @Test
    public void testGetUrlParameter() {
        String url = "http://www.baidu.com/?foo=bar&foz=";
        String key = "foz";
        String res = HoloTreeUtil.getUrlParameter(url, key);
        assertFalse(res == "");
        assertTrue(res.equals(""));
    }

    @Test
    public void testReferDecode() throws Exception {
        String refer = "http://s.etao.com/search?q=%B4%FC %C4%CD%BF%CB %D1%FC&nidpromote=4553772034%3A3&v=auction&stp=tb_search.srp.opsrh4.0.etaosrp&mt=1050&s=320";
        String refer_decode = HoloTreeUtil.unescape(refer);
        String true_refer = "http://s.etao.com/search?q=%B4%FC %C4%CD%BF%CB %D1%FC&nidpromote=4553772034:3&v=auction&stp=tb_search.srp.opsrh4.0.etaosrp&mt=1050&s=320";
        assertEquals(refer_decode, true_refer);
    }

    @Test
    public void testUrlDecode() {
        String url = "http://s.etao.com/search?q=\\xb4\\xfc \\xc4\\xcd\\xbf\\xcb \\xd1\\xfc&nidpromote=4553772034:3&v=auction&stp=tb_search.srp.opsrh4.0.etaosrp&mt=1050&s=0";
        String true_url = "http://s.etao.com/search?q=%B4%FC %C4%CD%BF%CB %D1%FC&nidpromote=4553772034:3&v=auction&stp=tb_search.srp.opsrh4.0.etaosrp&mt=1050&s=0";
        String url_decode = HoloTreeUtil.unescape(HoloTreeUtil.transformUrl(url));
        assertEquals(url_decode, true_url);
    }

    @Test
    public void testSimbaReferDecode() {
        String url = "http://tao.etao.com/auction?keyword=\\xce\\xc4\\xd0\\xd8&catid=50008881&refpid=tt_11402872_1272384_2783913&digest=DD1F1F9FB5C7DE4C030D37219BF460AF&crtid=108072826&itemid=16017508949&adgrid=102118014&eurl=http://click.simba.taobao.com/cc_im?p=&s=1971260182&k=288&e=Cu8tr0chKnVv7%2BOhedztQnBjEcl4O3tff8y7CdhZjf638WO3Aqo9uhkFdlrAszXJ40m3Qv7bJfob3IEMghT747OWd%2FhgiJKb3ZT2kJE0ShpLszPdlKL%2BnGcbGcFw%2BSvZnb%2F%2F%2FXYTIBzTPb7IjebtL9BwMnP2YKw2NE%2FJzm%2BS4JcoF3BJmEv8%2Fgurf7jCO3kXZvNRwLnsr8bVoDNi0%2BYvlIByA0%2FCCPJrOIBs8h4syFaWxr8aTepBt%2FSVjtTk17Fnt%2Fk0nfprxed9AdTuw6by%2Fzd6H66IpNSy&pid=tt_11402872_1272384_2783913&refpos=211_4764_22,n,i&pvid=ac1882404899502c9575000000441e60_0&clk1=8749169040b82e997656c0aa54267bd0";
        String refer = "http://tao.etao.com/auction?keyword=%CE%C4%D0%D8&catid=50008881&refpid=tt_11402872_1272384_2783913&digest=DD1F1F9FB5C7DE4C030D37219BF460AF&crtid=108072826&itemid=16017508949&adgrid=102118014&eurl=http%3A%2F%2Fclick.simba.taobao.com%2Fcc_im%3Fp%3D%26s%3D1971260182%26k%3D288%26e%3DCu8tr0chKnVv7%252BOhedztQnBjEcl4O3tff8y7CdhZjf638WO3Aqo9uhkFdlrAszXJ40m3Qv7bJfob3IEMghT747OWd%252FhgiJKb3ZT2kJE0ShpLszPdlKL%252BnGcbGcFw%252BSvZnb%252F%252F%252FXYTIBzTPb7IjebtL9BwMnP2YKw2NE%252FJzm%252BS4JcoF3BJmEv8%252Fgurf7jCO3kXZvNRwLnsr8bVoDNi0%252BYvlIByA0%252FCCPJrOIBs8h4syFaWxr8aTepBt%252FSVjtTk17Fnt%252Fk0nfprxed9AdTuw6by%252Fzd6H66IpNSy&pid=tt_11402872_1272384_2783913&refpos=211_4764_22,n,i&pvid=ac1882404899502c9575000000441e60_0&clk1=8749169040b82e997656c0aa54267bd0";
        String url_decode = HoloTreeUtil.unescape(HoloTreeUtil.transformUrl(url));
        String refer_decode = HoloTreeUtil.unescape(refer);
        assertEquals(url_decode, refer_decode);
    }

    @Test
    public void testTaoetaoReferRevise() {
        String url = "http://tao.etao.com/auction?keyword=\\xc3\\xc5\\xcc\\xfc/\\xd0\\xfe\\xb9\\xd8\\xb9\\xf1&catid=50015734&refpid=tt_10982364_973726_9023495&digest=A9EFE9D80D3FEA94EF86171F335EF588&crtid=124164394&itemid=13915376751&adgrid=117268367&eurl=http://click.simba.taobao.com/cc_im?p=&s=958614190&k=300&e=hliWpxq%2Bz4JzGCK0%2ButfPHu78qtrSuIggGLXow29oYMKGxuxPR3m%2B4KADm%2BLNDdsiDwXhUwFg5BqkqV42ywo%2FFRy1AUw0dOBgBFExp%2FmwRtsjwp9CyXmU8eUplGq8JqrxLenoyMmoRUKWGZ2aqhY7EEw59T8thn0Tg6Bew3cav580QAEAibrV8IjKLto22BDUSexWi9DiNA4bcvjTlDyjKf15Th6fQ79bD%2FFTR6D101yvauEYUfs%2BuEzXDmLzkXtLjv33npEXv0caz8%2FsxIfPiFpDgTfMeRlv4k%2B7POPpQA%3D&refpos=222_51543_10,n,i&clk1=d770dec5a6a895775f09c1a15980b658";
        String refer = "http://tao.etao.com/auction?keyword=%C3%C5%CC%FC%2F%D0%FE%B9%D8%B9%F1&catid=50015734&refpid=tt_10982364_973726_9023495&digest=A9EFE9D80D3FEA94EF86171F335EF588&crtid=124164394&itemid=13915376751&adgrid=117268367&eurl=http%3A%2F%2Fclick.simba.taobao.com%2Fcc_im%3Fp%3D%26s%3D958614190%26k%3D300%26e%3DhliWpxq%252Bz4JzGCK0%252ButfPHu78qtrSuIggGLXow29oYMKGxuxPR3m%252B4KADm%252BLNDdsiDwXhUwFg5BqkqV42ywo%252FFRy1AUw0dOBgBFExp%252FmwRtsjwp9CyXmU8eUplGq8JqrxLenoyMmoRUKWGZ2aqhY7EEw59T8thn0Tg6Bew3cav580QAEAibrV8IjKLto22BDUSexWi9DiNA4bcvjTlDyjKf15Th6fQ79bD%252FFTR6D101yvauEYUfs%252BuEzXDmLzkXtLjv33npEXv0caz8%252FsxIfPiFpDgTfMeRlv4k%252B7POPpQA%253D&refpos=222_51543_10,n,i&clk1=d770dec5a6a895775f09c1a15980b658";
        String url_decode = HoloTreeUtil.unescape(HoloTreeUtil.transformUrl(url));
        String true_refer = HoloTreeUtil.unescape(refer);
        assertEquals(url_decode, true_refer);
    }

    @Test
    public void testUnescape() {
        String a = "http%3A//list.taobao.com/market/nvzhuang2011a.htm%3Fspm%3Da2106.m874.1000190.7%26cat%3D50000697%26isprepay%3D1%26random%3Dfalse%26viewIndex%3D1%26yp4p_page%3D0%26commend%3Dall%26atype%3Db%26style%3Dgrid%26dtsp%3D1%26ppath%3D31609%3A103423%26olu%3Dyes%26isnew%3D%26smc%3D1";
        String expect = "http://list.taobao.com/market/nvzhuang2011a.htm?spm=a2106.m874.1000190.7&cat=50000697&isprepay=1&random=false&viewIndex=1&yp4p_page=0&commend=all&atype=b&style=grid&dtsp=1&ppath=31609:103423&olu=yes&isnew=&smc=1";
        String b = HoloTreeUtil.unescape(a);
        assertEquals(expect, b);

        a = "http%3A//s.taobao.com/search%3Fspm%3Da230r.1.10.324.b12ab3%26q%3Dcdma%25B5%25E7%25D0%25C5%25CA%25D6%25BB%25FA%26initiative_id%3Ditemz_20120906%26style%3Dgrid%26ssid%3Ds5-e%26bcoffset%3D1%26newpre%3Dnull%26s%3D240";
        expect = "http://s.taobao.com/search?spm=a230r.1.10.324.b12ab3&q=cdma%B5%E7%D0%C5%CA%D6%BB%FA&initiative_id=itemz_20120906&style=grid&ssid=s5-e&bcoffset=1&newpre=null&s=240";
        b = HoloTreeUtil.unescape(a);
        assertEquals(expect, b);

        a = "%25XX";
        expect = "%XX";
        b = HoloTreeUtil.unescape(a);
        assertEquals(expect, b);
    }

    @Test
    public void testDoubleUnescape() {
        String a = "%2526%2580%2a26";
        String expect = "&%80*26";
        String b = HoloTreeUtil.unescape(a);
        assertEquals(expect, b);
    }
}
