package com.ali.lz.effect.holotree;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.ali.lz.effect.exception.HoloConfigParserException;
import com.ali.lz.effect.exception.URLMatcherException;
import com.ali.lz.effect.proto.StarLogProtos;

public class URLMatcherTest {

    private static URLMatcher matcher;
    private static String confPath;

    @BeforeClass
    public static void setUp() throws Exception {
        HoloConfig conf = new HoloConfig();
        confPath = ClassLoader.getSystemResource("config.xml").getFile();
        conf.loadFile(confPath);
        matcher = new URLMatcher(conf);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        matcher = null;
    }

    @Test
    public void testLogEntry1() throws URLMatcherException {
        Map<String, Object> forMatch = new HashMap<String, Object>();
        forMatch.put("refer_url", "http://hi32.baidu.com?query=keys&dummy=no");
        PTLogEntry result = matcher.grep(forMatch);
        assertTrue(result.matched());
        assertEquals(result.getPType(), 0);
        assertEquals(result.getRType(), 1);
        assertEquals(result.get("keyword"), "keys");
        assertEquals(result.getSourceType("keyword"), HoloConfig.MATCH_REFERER);
        assertEquals(result.get("y"), "no");
        assertEquals(result.getSourceType("y"), HoloConfig.MATCH_REFERER);
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_UNKNOWN));
    }

    @Test
    public void testLogEntryPositive() throws URLMatcherException {
        StarLogProtos.FlowStarLog log_entry = StarLogProtos.FlowStarLog.newBuilder()
                .setUrl("http://www.baidu.com?query=keys&dummy=no")
                .setReferUrl("http://hi32.baidu.com?query=keys&dummy=no").build();
        PTLogEntry result = matcher.grep(log_entry);
        assertTrue(result.matched());
        assertEquals(result.getPType(), 0);
        assertEquals(result.getRType(), 1);
        assertEquals(result.get("keyword"), "keys");
        assertEquals(result.getSourceType("keyword"), HoloConfig.MATCH_REFERER);
        assertEquals(result.get("y"), "no");
        assertEquals(result.getSourceType("y"), HoloConfig.MATCH_REFERER);
        assertEquals(result.get("url"), log_entry.getUrl());
        assertEquals(result.getSourceType("url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("refer_url"), log_entry.getReferUrl());
        assertEquals(result.getSourceType("refer_url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_UNKNOWN));

        log_entry = StarLogProtos.FlowStarLog.newBuilder()
                .setUrl("http://www.qq.com?number=12345&dummy=no&nick=sucker")
                .setReferUrl("http://www.baidu.com?query=keys&dummy=no").build();
        result = matcher.grep(log_entry);
        assertTrue(result.matched());
        assertEquals(result.getPType(), 0);
        assertEquals(result.getRType(), 1);
        assertEquals(result.get("keyword"), "keys");
        assertEquals(result.getSourceType("keyword"), HoloConfig.MATCH_REFERER);
        assertEquals(result.get("y"), "no");
        assertEquals(result.getSourceType("y"), HoloConfig.MATCH_REFERER);
        assertEquals(result.get("url"), log_entry.getUrl());
        assertEquals(result.getSourceType("url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("refer_url"), log_entry.getReferUrl());
        assertEquals(result.getSourceType("refer_url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_UNKNOWN));

        log_entry = StarLogProtos.FlowStarLog
                .newBuilder()
                .setUrl("http://www.taobao.com?query=100&ali_refid=+?aword:die:103:death:huge&ali_trackid=1_000120021&fin=true")
                .setReferUrl("http://s.taobao.com/search?q=girl&cd_dw=1&initiative_id=staobaoz_20120609").build();
        result = matcher.grep(log_entry);
        assertTrue(result.matched());
        assertEquals(result.getPType(), 0);
        assertEquals(result.getRType(), 5);
        assertEquals(result.get("url"), log_entry.getUrl());
        assertEquals(result.getSourceType("url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("refer_url"), log_entry.getReferUrl());
        assertEquals(result.getSourceType("refer_url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_TAOBAO));
    }

    @Test
    public void testLogEntryNegative() throws URLMatcherException {
        StarLogProtos.FlowStarLog log_entry = StarLogProtos.FlowStarLog.newBuilder()
                .setUrl("http://www.sina.com?query=keys&dummy=no")
                .setReferUrl("http://hi32.sina.com?query=keys&dummy=no").build();
        PTLogEntry result = matcher.grep(log_entry);
        assertFalse(result.matched());
        assertEquals(result.getPType(), 0);
        assertEquals(result.getRType(), 0);
        assertEquals(result.get("keyword"), null);
        assertEquals(result.getSourceType("keyword"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("y"), null);
        assertEquals(result.getSourceType("y"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("url"), log_entry.getUrl());
        assertEquals(result.getSourceType("url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("refer_url"), log_entry.getReferUrl());
        assertEquals(result.getSourceType("refer_url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_UNKNOWN));

        log_entry = StarLogProtos.FlowStarLog.newBuilder().setUrl("http://www.etao.com?query=keys&dummy=no")
                .setReferUrl("http://s.etao.com?query=keys&dummy=no").build();
        result = matcher.grep(log_entry);
        assertFalse(result.matched());
        assertEquals(result.getPType(), 0);
        assertEquals(result.getRType(), 0);
        assertEquals(result.get("keyword"), null);
        assertEquals(result.getSourceType("keyword"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("y"), null);
        assertEquals(result.getSourceType("y"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("url"), log_entry.getUrl());
        assertEquals(result.getSourceType("url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("refer_url"), log_entry.getReferUrl());
        assertEquals(result.getSourceType("refer_url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_ETAO));
    }

    @Test
    public void testGeneratedConfig96() throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException, URLMatcherException {
        HoloConfig gconf = new HoloConfig();
        String gconfPath = ClassLoader.getSystemResource("report_96.xml").getFile();
        ;
        gconf.loadFile(gconfPath);
        URLMatcher gmatcher = new URLMatcher(gconf);

        StarLogProtos.FlowStarLog log_entry = StarLogProtos.FlowStarLog.newBuilder()
                .setUrl("http://taobao.com/effect_page.php").setReferUrl("http://etao.com/index.php").build();
        PTLogEntry result = gmatcher.grep(log_entry);
        assertTrue(result.matched());
        assertEquals(result.getPType(), 10000);
        assertEquals(result.getRType(), 115);
        assertEquals(result.get("url"), log_entry.getUrl());
        assertEquals(result.get("refer_url"), log_entry.getReferUrl());
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_TAOBAO));
    }

    @Test
    public void testGeneratedConfig97() throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException, URLMatcherException {
        HoloConfig gconf = new HoloConfig();
        String gconfPath = ClassLoader.getSystemResource("report_97.xml").getFile();
        ;
        gconf.loadFile(gconfPath);
        URLMatcher gmatcher = new URLMatcher(gconf);

        StarLogProtos.FlowStarLog log_entry = StarLogProtos.FlowStarLog.newBuilder()
                .setUrl("http://taobao.com?search=tb_market_id=100").setReferUrl("http://etao.com/index.php").build();
        PTLogEntry result = gmatcher.grep(log_entry);
        assertTrue(result.matched());
        assertEquals(result.getPType(), 0);
        assertEquals(result.getRType(), 100);
        assertEquals(result.get("url"), log_entry.getUrl());
        assertEquals(result.get("refer_url"), log_entry.getReferUrl());
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_TAOBAO));
    }

    @Test
    public void testGeneratedConfig11() throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException, URLMatcherException {
        HoloConfig gconf = new HoloConfig();
        String gconfPath = ClassLoader.getSystemResource("report_11.xml").getFile();
        ;
        gconf.loadFile(gconfPath);
        URLMatcher gmatcher = new URLMatcher(gconf);

        StarLogProtos.FlowStarLog log_entry = StarLogProtos.FlowStarLog
                .newBuilder()
                .setUrl("http://8.etao.com/wangshangshangcheng/bar-1310895-0-0-1-list.htm?bar_name=%CD%F8%C9%CF%C9%CC%B3%C7&q=http://3c.tmall.com/go/act/tmall/dqcksjac.php?spm=141.27931.264455.1&ad_id=&am_id=&cm_id=14010136042170e97f5e&pm_id=")
                .setReferUrl("http://etao.com/index.php").build();
        PTLogEntry result = gmatcher.grep(log_entry);
        assertTrue(result.matched());
        assertEquals(result.getPType(), 10000);
        assertEquals(result.getRType(), 103);
        assertEquals(result.get("url"), log_entry.getUrl());
        assertEquals(result.get("refer_url"), log_entry.getReferUrl());
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_ETAO));
    }

    @Test
    public void testGeneratedConfig26() throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException, URLMatcherException {
        HoloConfig gconf = new HoloConfig();
        String gconfPath = ClassLoader.getSystemResource("report_26.xml").getFile();
        ;
        gconf.loadFile(gconfPath);
        URLMatcher gmatcher = new URLMatcher(gconf);

        StarLogProtos.FlowStarLog log_entry = StarLogProtos.FlowStarLog
                .newBuilder()
                .setUrl("http://list.tmall.com/search_product.htm?spm=3.274416.251201.75&active=1&from=sn_1_cat&area_code=330100&navlog=11&nav=spu-cat&search_condition=7&style=g&sort=s&n=42&s=0&cat=50101180")
                .setReferUrl("").build();
        PTLogEntry result = gmatcher.grep(log_entry);
        assertTrue(result.matched());
        assertEquals(result.getPType(), 0);
        assertEquals(result.getRType(), 103);
        assertEquals(result.get("url"), log_entry.getUrl());
        assertEquals(result.getSourceType("url"), HoloConfig.MATCH_NONE);
        assertEquals(result.get("spm"), "3.274416.251201.75");
        assertEquals(result.getSourceType("spm"), HoloConfig.MATCH_REFERER);
        assertEquals(result.get("ali_corp"), new Integer(PTLogEntry.CORP_TMALL));
    }
}
