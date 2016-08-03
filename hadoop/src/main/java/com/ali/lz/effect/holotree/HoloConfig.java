package com.ali.lz.effect.holotree;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.ali.lz.effect.exception.HoloConfigParserException;
import com.ali.lz.effect.proto.StarLogProtos.FlowStarLog;
import com.google.protobuf.Descriptors.FieldDescriptor;

public class HoloConfig {

    // target_type字段的取值
    public final static int MATCH_NONE = 0;
    public final static int MATCH_URL = 1;
    public final static int MATCH_REFERER = 2;

    // 分组:
    // [0] referer/referer
    // [1] url/referer
    // [2] other/referer
    // [3] url/url
    // [4] other/url
    public final static int GROUP_REF_REF = 0;
    public final static int GROUP_URL_REF = 1;
    public final static int GROUP_OTH_REF = 2;
    public final static int GROUP_URL_URL = 3;
    public final static int GROUP_OTH_URL = 4;
    public final static int MAX_URL_RULE_GROUP = 5;

    public int ver;
    public int analyzer_id;
    public int plan_id;
    public int ttl;
    public int update_interval;
    public int period;
    public String tree_split_method;
    public String attr_calc_method;
    public int[] lookahead;
    // 单个 session 内允许用来建树的最大节点数，超过此数量时将会强行截断为多棵树
    public int maxSessNodes = 2000;

    // 是否全部输出节点，如为false则仅输出染色节点
    public boolean is_all = false;

    // 是否把所有的根节点认为是效果页（例:B2C和店铺经来源分析用到这种逻辑）
    public boolean root_is_lp = false;

    // 路径匹配染色标记,用于设置是否只建树不染色
    public boolean doPathMatch = true;

    // 建树前对日志分组的字段组合
    public List<String> treeGroupingFields = new ArrayList<String>();
    
    // 标示是否需要在matcher前对url、refer进行脱敏清洗
    public boolean doUrlMasking = false;
    
    // 标记是否计算黄金令箭效果
    public boolean doHjljOwnership = false;

    public int check_version = 2; // 支持的版本号，默认为2

    private Element xml_root;
    private ArrayList<UrlRule> url_rule = new ArrayList<UrlRule>();
    private ArrayList<PathRule> path_rule = new ArrayList<PathRule>();
    private ArrayList<Ind> effect = new ArrayList<Ind>();
    private ArrayList<ArrayList<UrlRule>> group = new ArrayList<ArrayList<UrlRule>>();

    /**
     * 配置文件规则检查说明
     * 1、所有的优先级priority值必须大于等于0
     * 2、src_path.rule.path_id必须为int类型
     * 3、url_type.rule.type_id必须大于等于0
     * 4、src_path.rule.effect_owner必须是src_path.rule.path.node节点id之一
     * 5、src_path.rule.limit.effect_id必须是effects.ind的id属性值之一
     * 6、src_path.rule.limit.num必须大于等于0
     * 7、ver值必须等于1
     * 
     * @author wuke.cj
     * 
     */

    public static final class MatchProfile {
        public String regexp; // 正则表达式
        public HashMap<String, Integer> props = new HashMap<String, Integer>();
    }

    public static final class UrlRule {
        public int priority; // 优先级
        public int type_id;
        public String match_field; // 匹配的目标字段，可以是任意字段
        public int target_type; // 匹配成功时要设置的目标类型，由MATCH_XXX常量决定
        public ArrayList<MatchProfile> match_regexps = new ArrayList<MatchProfile>(); // 正则表达式与捕获信息
        public ArrayList<MatchProfile> extract_regexps = new ArrayList<MatchProfile>();
        public FieldDescriptor match_field_fd; // match_field 对应的 PB 描述符
    }

    public static final class PathLimit {
        public int num;
        public int effect_id;
    }

    public static final class PathNode {
        public int id;
        public int next; // 下一路径节点的ID，"*"为-1，最后一个节点(没有next字段)为0
        public int[] type_refs; // 如果type_refs为"*"，type_refs[0] == -1
        public String expand; // 以哪个字段扩展，这个值对应的是MatchProfile.props定义的捕获信息。
    }

    public static final class PathRule {
        public int priority;
        public int path_id;
        public int effect_owner;
        public PathLimit limit;
        public ArrayList<PathNode> node; // 路径节点。
    }

    public static final class Ind {
        public int id;
        public int ind_id;
    }

    /**
     * 从文件构造一个HoloConfig对象
     * 
     * @param file_name
     *            文件名，从此文件读取配置文件
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     * @throws HoloConfigParserException
     */
    public HoloConfig(String file_name) throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException {
        loadFile(file_name);
    }

    /**
     * 默认的构造函数，构造一个空的HoloConfig对象
     */
    public HoloConfig() {
    }

    /**
     * 获得最大Session节点数目
     * 
     * @return
     */
    public int getMaxSessionNodes() {
        return maxSessNodes;
    }

    public void setMaxSessionNodes(int maxSessionNodes) {
        maxSessNodes = maxSessionNodes;
    }

    /**
     * 获取一个指定的URL规则配置信息，URL规则配置信息是一个以优先级从大到小排列的数组。
     * 
     * @param i
     *            URL规则数组的下标
     * @return 指定的URL规则配置信息
     */
    public UrlRule getUrlRule(int i) {
        return url_rule.get(i);
    }

    /**
     * @return 目前所有的URL规则数量
     */
    public int getUrlRuleCount() {
        return url_rule.size();
    }

    /**
     * 获取一个指定的Path规则配置信息，Path规则配置信息是一个以优先级从大到小排列的数组。
     * 
     * @param i
     *            Path规则数组的下标
     * @return 指定的Path规则配置信息
     */
    public PathRule getPathRule(int i) {
        return path_rule.get(i);
    }

    /**
     * @return 目前所有的Path规则配置数量
     */
    public int getPathRuleCount() {
        return path_rule.size();
    }

    public Ind getEffect(int i) {
        return effect.get(i);
    }

    public ArrayList<Ind> getEffect() {
        return effect;
    }

    public int getEffectCount() {
        return effect.size();
    }

    /**
     * @return 获取UrlRule的分组
     */
    public ArrayList<UrlRule> getGroup(int i) {
        return group.get(i);
    }

    public ArrayList<ArrayList<UrlRule>> getGroup() {
        return group;
    }

    /**
     * 读取xml内容文件，初始化配置对象
     * 
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws HoloConfigParserException
     * 
     */
    public void loadFile(String file_name) throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException {
        InputStream is = new FileInputStream(file_name);
        load(is);
        is.close();
    }

    /**
     * 读取xml内容字符串，初始化配置对象
     * 
     * @param xmlContent
     *            xml内容字符串
     * @throws IOException
     * @throws SAXException
     * @throws ParserConfigurationException
     * @throws HoloConfigParserException
     */
    public void loadString(String xmlContent) throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException {
        InputStream is = new ByteArrayInputStream(xmlContent.getBytes());
        load(is);
        is.close();
    }

    /**
     * 读取XML配置文件，并创建配置信息
     * 
     * @param is
     *            输入流
     * @throws ParserConfigurationException
     * @throws SAXException
     * @throws IOException
     * @throws HoloConfigParserException
     *             规则解析异常
     */
    public void load(InputStream is) throws ParserConfigurationException, SAXException, IOException,
            HoloConfigParserException {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        DocumentBuilder db = dbf.newDocumentBuilder();
        xml_root = db.parse(is).getDocumentElement();
        readRoot();
        makeUrlRuleGroup();
        Collections.sort(url_rule, new Comparator<UrlRule>() {
            @Override
            public int compare(UrlRule lhs, UrlRule rhs) {
                return rhs.priority - lhs.priority;
            }
        });
        Collections.sort(path_rule, new Comparator<PathRule>() {
            @Override
            public int compare(PathRule lhs, PathRule rhs) {
                return rhs.priority - lhs.priority;
            }
        });
    }

    private void readRoot() throws HoloConfigParserException {
        // 读取XML文档中第一层的数据
        ver = fetchNodeInt(xml_root, "ver");
        if (ver > check_version) // 强制检查版本号
            throw new HoloConfigParserException("version: " + ver + " is not support now");
        analyzer_id = fetchNodeInt(xml_root, "analyzer_id");
        plan_id = fetchNodeInt(xml_root, "plan_id");
        ttl = fetchNodeInt(xml_root, "ttl");
        update_interval = fetchNodeInt(xml_root, "update_interval");
        period = fetchNodeInt(xml_root, "period");
        readUrlType(); // 读取主要的节点
        readSrcPath();
        readEffects();
        readMisc(); // 读取杂项信息
        readLookahead(); // 2.0新增：指定跳数
        readTreeBuilderConf(); // 读取建树规则
    }

    private void readUrlType() throws HoloConfigParserException {
        NodeList node = xml_root.getElementsByTagName("url_type");
        if (node.getLength() == 1 && node.item(0).getNodeType() == Node.ELEMENT_NODE) {
            NodeList children = ((Element) node.item(0)).getElementsByTagName("rule");
            for (int i = 0; i < children.getLength(); ++i) {
                url_rule.add(readUrlRule(children.item(i)));
            }
        }
    }

    private UrlRule readUrlRule(Node node) throws HoloConfigParserException {
        Element elem = (Element) node;
        UrlRule rv = new UrlRule();
        rv.priority = fetchNodeInt(elem, "priority");
        if (rv.priority < 0)
            throw new HoloConfigParserException("<url_type>.<rule>.<priority> must >= 0");

        rv.type_id = fetchNodeInt(elem, "type_id");
        if (rv.type_id < 0)
            throw new HoloConfigParserException("<url_type>.<rule>.<type_id> must >= 0");

        rv.match_field = fetchNodeString(elem, "match_field");
        if ("referer".equals(rv.match_field))
            rv.match_field = "refer_url";

        rv.match_field_fd = FlowStarLog.getDescriptor().findFieldByName(rv.match_field);

        rv.target_type = literalToMatchType(fetchNodeStringOrN(elem, "target_type"));

        NodeList children = elem.getElementsByTagName("match_regexps");
        if (children.getLength() > 0) {
            children = ((Element) children.item(0)).getElementsByTagName("match_regexp");
            for (int i = 0; i < children.getLength(); ++i) {
                rv.match_regexps.add(readRegexp(children.item(i)));
            }
        }
        children = elem.getElementsByTagName("extract_regexps");
        if (children.getLength() > 0) {
            children = ((Element) children.item(0)).getElementsByTagName("extract_regexp");
            for (int i = 0; i < children.getLength(); ++i) {
                rv.extract_regexps.add(readRegexp(children.item(i)));
            }
        }
        return rv;
    }

    private MatchProfile readRegexp(Node node) throws HoloConfigParserException {
        MatchProfile rv = new MatchProfile();
        Element elem = (Element) node;
        rv.regexp = fetchNodeString(elem, "regexp");
        NodeList children = elem.getElementsByTagName("props");
        if (children.getLength() == 1)
            readProps(children.item(0), rv.props);
        return rv;
    }

    private void readProps(Node node, HashMap<String, Integer> map) throws HoloConfigParserException {
        NodeList children = ((Element) node).getElementsByTagName("prop");
        for (int i = 0; i < children.getLength(); ++i) {
            if ("prop".equals(children.item(i).getNodeName())) {
                Element prop = (Element) children.item(i);
                String key = fetchAttrString(prop, "field");
                String value = fetchAttrString(prop, "value").substring(1);
                map.put(key, new Integer(value));
            }
        }
    }

    private void readSrcPath() throws HoloConfigParserException {
        NodeList node = xml_root.getElementsByTagName("src_path");
        if (node.getLength() == 1 && node.item(0).getNodeType() == Node.ELEMENT_NODE) {
            NodeList children = ((Element) node.item(0)).getElementsByTagName("rule");
            for (int i = 0; i < children.getLength(); ++i) {
                path_rule.add(readPathRule(children.item(i)));
            }
        }
        if (path_rule.size() == 0)
            throw new HoloConfigParserException("No any \"src_path\" node for path rules.");
    }

    private PathRule readPathRule(Node node) throws HoloConfigParserException {
        Element elem = (Element) node;
        PathRule rule = new PathRule();
        rule.priority = fetchNodeInt(elem, "priority");
        rule.path_id = fetchNodeInt(elem, "path_id");
        rule.effect_owner = fetchNodeInt(elem, "effect_owner");
        rule.limit = readPathLimit(elem.getElementsByTagName("limit").item(0));
        rule.node = readPath(elem.getElementsByTagName("path").item(0));
        boolean chk = false; // 检查
        for (PathNode i : rule.node) {
            if (rule.effect_owner == i.id) {
                chk = true;
                break;
            }
        }
        if (!chk)
            throw new HoloConfigParserException("effect_owner:" + rule.effect_owner + "is not in path nodes");
        return rule;
    }

    private PathLimit readPathLimit(Node node) throws HoloConfigParserException {
        Element elem = (Element) node;
        PathLimit rv = new PathLimit();
        rv.num = fetchNodeInt(elem, "num");
        if (rv.num < 0)
            throw new HoloConfigParserException("<src_path>.<rule>.<limit>.<num> must >= 0");
        rv.effect_id = fetchNodeInt(elem, "effect_id");
        return rv;
    }

    private ArrayList<PathNode> readPath(Node node) throws HoloConfigParserException {
        Element elem = (Element) node;
        ArrayList<PathNode> rv = null;
        NodeList children = elem.getElementsByTagName("node");
        if (children.getLength() > 0)
            rv = new ArrayList<PathNode>();
        for (int i = 0; i < children.getLength(); ++i)
            rv.add(readPathNode(children.item(i)));
        return rv;
    }

    private PathNode readPathNode(Node node) throws HoloConfigParserException {
        Element elem = (Element) node;
        PathNode rv = new PathNode();
        rv.id = fetchAttrInt(elem, "id");
        rv.next = fetchAttIntOrN(elem, "next");
        rv.expand = fetchAttrStringOrDefault(elem, "expand", "rule");
        String type_refs = fetchAttrString(elem, "type_refs");
        if (type_refs.isEmpty())
            throw new HoloConfigParserException("src_path.path.node.type_refs can not be empty");

        String[] splited = type_refs.split(",");
        rv.type_refs = new int[splited.length];
        if ("*".equals(type_refs)) {
            rv.type_refs[0] = -1;
        } else {
            for (int i = 0; i < splited.length; ++i)
                rv.type_refs[i] = Integer.parseInt(splited[i]);
        }
        return rv;
    }

    private void readEffects() throws HoloConfigParserException {
        Element elem = (Element) xml_root.getElementsByTagName("effects").item(0);
        NodeList children = elem.getElementsByTagName("ind");
        for (int i = 0; i < children.getLength(); ++i) {
            Element node = (Element) children.item(i);
            Ind ind = new Ind();
            ind.id = fetchAttrInt(node, "id");
            ind.ind_id = fetchAttrInt(node, "ind_id");
            effect.add(ind);
        }
    }

    private void readMisc() throws HoloConfigParserException {
        Element elem = (Element) xml_root.getElementsByTagName("tree_split").item(0);
        tree_split_method = fetchAttrString(elem, "method");
        elem = (Element) xml_root.getElementsByTagName("attr_calc").item(0);
        attr_calc_method = fetchAttrString(elem, "method");

        String value = fetchNodeStringOrN(xml_root, "is_all");
        if (value != null) {
            is_all = Integer.parseInt(value) == 0 ? false : true;
        }

        value = fetchNodeStringOrN(xml_root, "root_is_lp");
        if (value != null) {
            root_is_lp = Integer.parseInt(value) == 0 ? false : true;
        }

        if (root_is_lp) {
            is_all = root_is_lp;
        }
        
        value = fetchNodeStringOrN(xml_root, "url_masking_tag");
        if (value != null) {
            doUrlMasking = Integer.parseInt(value) == 0? false : true;
        }
        
        value = fetchNodeStringOrN(xml_root, "hjlj_tag");
        if (value != null) {
            doHjljOwnership = Integer.parseInt(value) == 0? false : true;
        }
    }

    private void readTreeBuilderConf() throws HoloConfigParserException {

        String maxSessionNodes = fetchNodeStringOrN(xml_root, "max_session_nodes");
        if (maxSessionNodes != null && Integer.parseInt(maxSessionNodes) > 0) {
            maxSessNodes = Integer.parseInt(maxSessionNodes);
        }

        Element elem = (Element) xml_root.getElementsByTagName("tree_grouping_fields").item(0);
        if (elem != null) {
            NodeList children = elem.getElementsByTagName("fields");
            for (int i = 0; i < children.getLength(); ++i) {
                Element node = (Element) children.item(i);
                treeGroupingFields.add(fetchAttrInt(node, "id"), fetchAttrString(node, "name"));
            }
        } else {
            // 默认分组规则为cookie
            treeGroupingFields.add(0, "cookie");
        }

        String value = fetchNodeStringOrN(xml_root, "do_path_match");
        if (value != null) {
            doPathMatch = Integer.parseInt(value) == 0 ? false : true;
        }
    }

    private void readLookahead() throws HoloConfigParserException {
        NodeList list = xml_root.getElementsByTagName("lookahead");
        if (list == null || list.getLength() == 0) { // 没有指定lookahead字段的时候，默认为1
            lookahead = new int[1];
            lookahead[0] = 1;
            return;
        }
        Element elem = (Element) list.item(0);
        // 去重并有序的将结果放入lookahead中。
        Set<Integer> jumpNums = new TreeSet<Integer>();
        for (String i : elem.getTextContent().split(",")) {
            jumpNums.add(Integer.parseInt(i));
        }
        if (jumpNums.size() == 0) {
            lookahead = new int[1];
            lookahead[0] = 1;
            return;
        }
        lookahead = new int[jumpNums.size()];
        int p = 0;
        for (Integer i : jumpNums) {
            lookahead[p++] = i;
        }
    }

    private void makeUrlRuleGroup() {
        for (int i = 0; i < MAX_URL_RULE_GROUP; ++i)
            group.add(new ArrayList<UrlRule>());
        // 按照match_file/target_type字段分组
        for (UrlRule rule : url_rule) {
            if (rule.target_type == MATCH_REFERER) {
                if ("refer_url".equals(rule.match_field))
                    group.get(GROUP_REF_REF).add(rule); // Referer/Referer
                else if ("url".equals(rule.match_field))
                    group.get(GROUP_URL_REF).add(rule); // Url/Referer
                else
                    group.get(GROUP_OTH_REF).add(rule); // Other/Referer
            } else if (rule.target_type == MATCH_URL) {
                if ("url".equals(rule.match_field))
                    group.get(GROUP_URL_URL).add(rule); // Url/Url
//                else if ("url".equals(rule.match_field) && "refer_url".equals(rule.match_field))
                else
                    group.get(GROUP_OTH_URL).add(rule); // Other/Url
            } else
                assert (false); // 不可到达!
        }
        // 对每个分组按优先级从大到小排序
        for (ArrayList<UrlRule> list : group)
            Collections.sort(list, new Comparator<UrlRule>() {
                @Override
                public int compare(UrlRule lhs, UrlRule rhs) {
                    return rhs.priority - lhs.priority;
                }
            });
    }

    /*
     * 杂项函数
     */
    private static int literalToMatchType(final String literal) throws HoloConfigParserException {
        int rv = -1;
        if (literal == null)
            rv = MATCH_URL;
        else if (literal.equals("url"))
            rv = MATCH_URL;
        else if (literal.equals("referer"))
            rv = MATCH_REFERER;
        else
            throw new HoloConfigParserException("type literal: \"" + literal + "\" is not be support");
        return rv;
    }

    private static int fetchNodeInt(Element parent, String name) throws HoloConfigParserException {
        Node node = parent.getElementsByTagName(name).item(0);
        if (node == null)
            throw new HoloConfigParserException("Can not found \"" + name + "\"");
        String str = node.getTextContent();
        return Integer.parseInt(str);
    }

    private static String fetchNodeString(Element parent, String name) throws HoloConfigParserException {
        Node node = parent.getElementsByTagName(name).item(0);
        if (node == null)
            throw new HoloConfigParserException("Can not found \"" + name + "\"");
        String str = node.getTextContent();
        return str;
    }

    private static String fetchNodeStringOrN(Element parent, String name) {
        Node node = parent.getElementsByTagName(name).item(0);
        if (node == null)
            return null;
        String str = node.getTextContent();
        return str;
    }

    private static int fetchAttrInt(Element elem, String name) throws HoloConfigParserException {
        Attr attr = elem.getAttributeNode(name);
        if (attr == null)
            throw new HoloConfigParserException("Can not found \"" + name + "\"");
        String literal = attr.getValue();
        return Integer.parseInt(literal);
    }

    private static int fetchAttIntOrN(Element elem, String name) {
        Attr attr = elem.getAttributeNode(name);
        if (attr == null)
            return 0;
        if (attr.getValue().equals("*"))
            return -1;
        return Integer.parseInt(attr.getValue());
    }

    private static String fetchAttrString(Element elem, String name) throws HoloConfigParserException {
        Attr attr = elem.getAttributeNode(name);
        if (attr == null)
            throw new HoloConfigParserException("Can not found \"" + name + "\"");
        return attr.getValue();
    }

    private static String fetchAttrStringOrDefault(Element elem, String name, String def) {
        Attr attr = elem.getAttributeNode(name);
        if (attr == null)
            return def;
        return attr.getValue();
    }
}
