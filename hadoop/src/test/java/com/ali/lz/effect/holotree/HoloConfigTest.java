package com.ali.lz.effect.holotree;

import static org.junit.Assert.*;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.xml.sax.SAXException;

import com.ali.lz.effect.exception.HoloConfigParserException;
import com.ali.lz.effect.holotree.HoloConfig;

/**
 * @author wuke.cj
 * 
 */
public class HoloConfigTest {

    private HoloConfig conf;
    private String confPath;

    @Before
    public void setUp() throws Exception {
        confPath = ClassLoader.getSystemResource("config.xml").getFile();
        conf = new HoloConfig();
        conf.loadFile(confPath);
    }

    @After
    public void tearDown() throws Exception {
        conf = null;
    }

    @Test
    public void test1stLayer() {
        assertEquals(conf.ver, 2);
        assertEquals(conf.analyzer_id, 123);
        assertEquals(conf.plan_id, 1);
        assertEquals(conf.period, 3);
        assertEquals(conf.ttl, 3);
        assertEquals(conf.update_interval, 3600);

        assertEquals(conf.tree_split_method, "ali:etao,taobao");
        assertEquals(conf.attr_calc_method, "last");
        assertEquals(conf.lookahead[0], 2);
        assertEquals(conf.lookahead[1], 3);
        assertEquals(conf.lookahead[2], 4);
        assertEquals(conf.lookahead[3], 5);
        assertEquals(conf.getMaxSessionNodes(), 1000);
        assertEquals(conf.treeGroupingFields.get(0), "cookie");
        assertEquals(conf.treeGroupingFields.get(1), "session");
    }

    @Test
    public void testUrlRule() {
        assertEquals(conf.getUrlRuleCount(), 3);

        HoloConfig.UrlRule rule = conf.getGroup(HoloConfig.GROUP_REF_REF).get(0);
        assertEquals(rule.match_field, "refer_url");
        assertEquals(rule.target_type, HoloConfig.MATCH_REFERER);
        assertEquals(rule.priority, 10);
        assertEquals(rule.type_id, 1);
        assertEquals(rule.match_regexps.get(0).regexp, "http://.*\\.baidu\\.com\\?query=([^&]*)&dummy=([^&]*)");
        assertEquals(rule.match_regexps.get(0).props.get("keyword"), new Integer(1));
        assertEquals(rule.match_regexps.get(0).props.get("y"), new Integer(2));

        rule = conf.getGroup(HoloConfig.GROUP_REF_REF).get(1);
        assertEquals(rule.match_field, "refer_url");
        assertEquals(rule.target_type, HoloConfig.MATCH_REFERER);
        assertEquals(rule.priority, 10);
        assertEquals(rule.type_id, 2);
        assertEquals(rule.match_regexps.get(0).regexp, "http://.*\\.google\\.com\\?query=([^&]*)&foo=([^&]*)");
        assertEquals(rule.match_regexps.get(0).props.get("keyword"), new Integer(1));
        assertEquals(rule.match_regexps.get(0).props.get("y"), new Integer(2));

        rule = conf.getGroup(HoloConfig.GROUP_URL_REF).get(0);
        assertEquals(rule.match_field, "url");
        assertEquals(rule.target_type, HoloConfig.MATCH_REFERER);
        assertEquals(rule.priority, 1);
        assertEquals(rule.type_id, 5);
        assertEquals(rule.match_regexps.get(0).regexp, "ali_trackid=1_");
        assertEquals(rule.match_regexps.get(0).props.size(), 0);
        assertEquals(rule.extract_regexps.get(0).regexp, "ali_refid=.+:\\w+:\\d+:(.*?):");
        assertEquals(rule.extract_regexps.get(0).props.get("refid"), new Integer(1));
    }

    @Test
    public void testPathRule() {
        assertEquals(conf.getPathRuleCount(), 2);

        HoloConfig.PathRule rule = conf.getPathRule(0);
        assertEquals(rule.priority, 10);
        assertEquals(rule.path_id, 1);
        assertEquals(rule.effect_owner, 0);
        assertEquals(rule.limit.effect_id, 1);
        assertEquals(rule.limit.num, 1000);
        assertEquals(rule.node.get(0).id, 0);
        assertEquals(rule.node.get(0).next, 1);
        assertEquals(rule.node.get(0).expand, ":keyword");
        assertEquals(rule.node.get(0).type_refs.length, 1);
        assertEquals(rule.node.get(0).type_refs[0], 1);
        assertEquals(rule.node.get(1).id, 1);
        assertEquals(rule.node.get(1).next, 0);
        assertEquals(rule.node.get(1).expand, "rule");
        assertEquals(rule.node.get(1).type_refs.length, 1);
        assertEquals(rule.node.get(1).type_refs[0], -1);

        rule = conf.getPathRule(1);
        assertEquals(rule.priority, 10);
        assertEquals(rule.path_id, 1);
        assertEquals(rule.effect_owner, 0);
        assertEquals(rule.limit.effect_id, 1);
        assertEquals(rule.limit.num, 1000);
        assertEquals(rule.node.get(0).id, 0);
        assertEquals(rule.node.get(0).next, 1);
        assertEquals(rule.node.get(0).expand, ":keyword");
        assertEquals(rule.node.get(0).type_refs.length, 1);
        assertEquals(rule.node.get(0).type_refs[0], 2);
        assertEquals(rule.node.get(1).id, 1);
        assertEquals(rule.node.get(1).next, 0);
        assertEquals(rule.node.get(1).expand, "rule");
        assertEquals(rule.node.get(1).type_refs.length, 1);
        assertEquals(rule.node.get(1).type_refs[0], -1);
    }

    @Test
    public void testEffects() {
        assertEquals(conf.getEffectCount(), 3);
        assertEquals(conf.getEffect(0).id, 0);
        assertEquals(conf.getEffect(0).ind_id, 103);
        assertEquals(conf.getEffect(1).id, 1);
        assertEquals(conf.getEffect(1).ind_id, 104);
        assertEquals(conf.getEffect(2).id, 2);
        assertEquals(conf.getEffect(2).ind_id, 205);
    }

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Test
    public void testNoPathNodeException() throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException {

        expected.expect(HoloConfigParserException.class);
        expected.expectMessage("No any \"src_path\" node for path rules.");
        HoloConfig bad = new HoloConfig();
        bad.loadString("<effect_plan>" + "<ver>2</ver>" + "<analyzer_id>123</analyzer_id>" + "<plan_id>1</plan_id>"
                + "<ttl>3</ttl>" + "<update_interval>3600</update_interval>" + "<period>3</period>" + "<url_type>"
                + "</url_type>" + "</effect_plan>");
    }

    @Test
    public void testBadVersionException() throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException {
        expected.expect(HoloConfigParserException.class);
        expected.expectMessage("version: 2012 is not support now");
        HoloConfig bad = new HoloConfig();
        bad.loadString("<effect_plan>" + "<ver>2012</ver>" + "<analyzer_id>123</analyzer_id>" + "<plan_id>1</plan_id>"
                + "<ttl>3</ttl>" + "<update_interval>3600</update_interval>" + "<period>3</period>" + "<url_type>"
                + "</url_type>" + "<src_path>" + "<rule>" + "  <priority>10</priority>" + "    <limit>"
                + "        <num>1000</num>" + "        <effect_id>1</effect_id>" + "    </limit>"
                + "    <path_id>1</path_id>" + "    <path>"
                + "        <node id=\"0\" type_refs=\"1\" next=\"1\" expand=\":keyword\" />"
                + "        <node id=\"1\" type_refs=\"*\" />" + "    </path>" + "    <effect_owner>0</effect_owner>"
                + "</rule>" + "</src_path>" + "<effects>" + "		<ind id=\"1\" ind_id=\"104\" />" + "</effects>"
                + "</effect_plan>");
    }

    @Test
    public void testBadNoVersionException() throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException {
        expected.expect(HoloConfigParserException.class);
        expected.expectMessage("Can not found \"ver\"");
        HoloConfig bad = new HoloConfig();
        bad.loadString("<effect_plan>"
                // + "<ver>2012</ver>"
                + "<analyzer_id>123</analyzer_id>" + "<plan_id>1</plan_id>" + "<ttl>3</ttl>"
                + "<update_interval>3600</update_interval>" + "<period>3</period>" + "<url_type>" + "</url_type>"
                + "<src_path>" + "<rule>" + "  <priority>10</priority>" + "    <limit>" + "        <num>1000</num>"
                + "        <effect_id>1</effect_id>" + "    </limit>" + "    <path_id>1</path_id>" + "    <path>"
                + "        <node id=\"0\" type_refs=\"1\" next=\"1\" expand=\":keyword\" />"
                + "        <node id=\"1\" type_refs=\"*\" />" + "    </path>" + "    <effect_owner>0</effect_owner>"
                + "</rule>" + "</src_path>" + "<effects>" + "		<ind id=\"1\" ind_id=\"104\" />" + "</effects>"
                + "</effect_plan>");
    }
}
