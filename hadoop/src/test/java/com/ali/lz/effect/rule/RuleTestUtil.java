package com.ali.lz.effect.rule;

import static org.easymock.EasyMock.expect;
import static org.easymock.classextension.EasyMock.createStrictControl;
import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.easymock.classextension.IMocksControl;
import static org.easymock.classextension.EasyMock.*;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Represent;
import org.yaml.snakeyaml.representer.Representer;

import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.holotree.SourceMeta;
import com.ali.lz.effect.holotree.HoloConfig.PathNode;
import com.ali.lz.effect.holotree.HoloConfig.PathRule;
import com.ali.lz.effect.rule.RuleSet;

public class RuleTestUtil {

    /**
     * 读取yaml文件，根据rules配置mock HoloConfig生成规则，读入input，生成的结果匹配output
     * 
     * @param resPath
     *            yaml文件
     * @param caseName
     *            case的名字，与id对应，如果为null，跑yaml所有的case
     * @param dump
     *            如果为true，则不会对比ouput，而是dump到控制台
     * @throws FileNotFoundException
     */
    @SuppressWarnings("unchecked")
    public static void testDriver(String resPath, String caseName, boolean dump) throws FileNotFoundException {

        Yaml yaml = new Yaml();
        List<Map<String, Object>> config = (List<Map<String, Object>>) yaml.load(new InputStreamReader(ClassLoader
                .getSystemResourceAsStream(resPath)));

        for (Map<String, Object> caseConfig : config) {
            if (!caseConfig.containsKey("id")) {
                fail("Invalid config, must have id");
            }

            String id = (String) caseConfig.get("id");
            if (caseName != null && id.equals(caseName)) {
                assertEqualCase(id, caseConfig, dump);
                break;
            }

            if (caseName == null) {
                assertEqualCase(id, caseConfig, dump);
            }

        }
    }

    @SuppressWarnings("unchecked")
    public static void assertEqualCase(String id, Map<String, Object> caseConfig, boolean dump) {

        if (!caseConfig.containsKey("rules")) {
            fail(id + " : Invalid config, must have rules");
        }

        List<Map<String, Object>> ruleConfigs = (List<Map<String, Object>>) caseConfig.get("rules");
        assertNotNull(ruleConfigs);

        List<PathRule> pathRules = genPathRules(id, ruleConfigs);

        // Mock HoloConfig
        IMocksControl ctrl = createStrictControl();

        HoloConfig holoConf = ctrl.createMock(HoloConfig.class);

        expect(holoConf.getPathRuleCount()).andReturn(pathRules.size());
        for (int i = 0; i < pathRules.size(); i++) {
            expect(holoConf.getPathRule(eq(i))).andReturn(pathRules.get(i));
        }

        // replay
        ctrl.replay();

        RuleSet rs = new RuleSet(holoConf);

        List<Integer> effectPages = (List<Integer>) caseConfig.get("ep");
        assertNotNull(id + " : Invalid config, must have ep", effectPages);
        if (dump) {
            dumpEffectPageToYaml(id, rs);
        } else {
            assertEquals(id + " : ep size does not match", effectPages.size(), rs.getEffectPageSet().size());
            assertTrue(id + " : ep does not match", rs.getEffectPageSet().containsAll(effectPages));
        }

        ctrl.verify();
        ctrl.reset();

        List<Map<String, Object>> io = (List<Map<String, Object>>) caseConfig.get("io");
        assertNotNull(id + " : Invalid config, must have io", io);

        for (Map<String, Object> ioConfig : io) {

            List<Map<String, Object>> inputConfig = (List<Map<String, Object>>) ioConfig.get("input");

            assertNotNull(id + " : Invalid config, io must have input", inputConfig);

            List<HoloTreeNode> input = loadInput(id, inputConfig);

            matchInput(input, rs);

            rs.reset();
            if (dump) {
                dumpResultsToYaml(inputConfig, input);
            } else {
                List<List<Map<String, Object>>> output = (List<List<Map<String, Object>>>) ioConfig.get("output");
                assertNotNull(id + " : Invalid config, io must have output", output);
                assertEqualOutput(id, input, output);
            }
        }
    }

    public static void dumpEffectPageToYaml(String id, RuleSet rs) {
        DumperOptions opts = new DumperOptions();
        opts.setAllowUnicode(false);
        opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);

        Yaml yaml = new Yaml(new MyRepresenter(), opts);

        List<Integer> ep = new ArrayList<Integer>();

        for (int e : rs.getEffectPageSet()) {
            ep.add(e);
        }

        Map<String, Object> m = new HashMap<String, Object>();
        m.put("id", id);
        m.put("ep", ep);

        List<Object> l = new ArrayList<Object>();
        l.add(m);
        System.out.println(yaml.dump(l));
    }

    public static void assertEqualOutput(String id, List<HoloTreeNode> nodes, List<List<Map<String, Object>>> output) {

        assertEquals(id + ": size of results does not match size of output", nodes.size(), output.size());

        int i = 0;

        for (List<Map<String, Object>> srcList : output) {

            HoloTreeNode node = nodes.get(i);

            Map<String, SourceMeta> sources = node.getSources();

            assertEquals(id + ": size of sources does not match", sources.size(), srcList.size());

            for (Map<String, Object> src : srcList) {
                String source = (String) src.get("src");

                assertTrue(id + ": src does not exists", sources.containsKey(source));
                SourceMeta meta = sources.get(source);

                assertEquals(id + ": firstOpTS does not match", ((Integer) src.get("firstOpTS")).longValue(),
                        meta.getFirstOpTS());
                assertEquals(id + ": lastOpTS does not match", ((Integer) src.get("lastOpTS")).longValue(),
                        meta.getLastOpTS());
                assertEquals(id + ": priority does not match", src.get("priority"), meta.getPriority());
                assertEquals(id + ": firstEP does not match", src.get("firstEP"), 0);
                assertEquals(id + ": lastEP does not match", src.get("lastEP"), 0);

            }

            i++;
        }
    }

    public static void dumpResultsToYaml(List<Map<String, Object>> inputConfig, List<HoloTreeNode> nodes) {
        DumperOptions opts = new DumperOptions();
        opts.setAllowUnicode(false);
        opts.setDefaultScalarStyle(DumperOptions.ScalarStyle.PLAIN);
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);

        Yaml yaml = new Yaml(new MyRepresenter(), opts);

        List<List<Map<String, Object>>> outs = new LinkedList<List<Map<String, Object>>>();
        for (HoloTreeNode node : nodes) {

            Map<String, SourceMeta> sources = node.getSources();

            List<Map<String, Object>> srcList = new LinkedList<Map<String, Object>>();

            for (Entry<String, SourceMeta> entry : sources.entrySet()) {
                Map<String, Object> src = new HashMap<String, Object>();
                src.put("src", entry.getKey());
                SourceMeta meta = entry.getValue();
                src.put("firstOpTS", meta.getFirstOpTS());
                src.put("lastOpTS", meta.getLastOpTS());
                src.put("priority", meta.getPriority());
                src.put("firstEP", nodes.lastIndexOf(meta.getFirstEP()));
                src.put("lastEP", nodes.lastIndexOf(meta.getLastEP()));
                srcList.add(src);
            }
            outs.add(srcList);
        }

        Map<String, Object> m = new HashMap<String, Object>();
        m.put("input", inputConfig);
        m.put("output", outs);

        Map<String, Object> io = new HashMap<String, Object>();
        List<Object> l = new LinkedList<Object>();
        l.add(m);
        io.put("io", l);

        l = new LinkedList<Object>();
        l.add(io);

        System.out.println(yaml.dump(l));
    }

    public static void matchInput(List<HoloTreeNode> input, RuleSet rs) {

        if (input.size() > 0) {
            boolean needMore = true;
            HoloTreeNode node = input.get(0);
            while (needMore) {
                needMore = rs.matchNext(node);
                if (node != null) {
                    node = node.getParent();
                }
            }
        }
    }

    public static List<HoloTreeNode> loadInput(String id, List<Map<String, Object>> inputConfig) {
        List<HoloTreeNode> nodes = new ArrayList<HoloTreeNode>();

        assertTrue(inputConfig.size() > 0);

        HoloTreeNode lastNode = null;

        for (Map<String, Object> i : inputConfig) {
            if (!i.containsKey("ptype") || !i.containsKey("rtype")) {
                fail(id + ": Invalid config, must have ptype and rtype");
            }
            HoloTreeNode treeNode = new HoloTreeNode(new PTLogEntry());
            treeNode.getPtLogEntry().setPType((Integer) i.get("ptype"));
            treeNode.getPtLogEntry().setRType((Integer) i.get("rtype"));
            treeNode.getPtLogEntry().put("ts", ((Integer) i.get("ts")).longValue());

            if (i.containsKey("prop")) {
                @SuppressWarnings("unchecked")
                Map<String, String> props = (Map<String, String>) i.get("prop");
                for (Entry<String, String> entry : props.entrySet()) {
                    treeNode.getPtLogEntry().put(entry.getKey(), entry.getValue());
                }
            }

            if (i.containsKey("sourceType")) {
                @SuppressWarnings("unchecked")
                Map<String, Integer> sourceTypes = (Map<String, Integer>) i.get("sourceType");
                for (Entry<String, Integer> entry : sourceTypes.entrySet())
                    treeNode.getPtLogEntry().putSourceType(entry.getKey(), entry.getValue());
            }
            treeNode.setParent(null);
            nodes.add(treeNode);

            if (lastNode != null) {
                lastNode.setParent(treeNode);
            }

            lastNode = treeNode;
        }

        return nodes;
    }

    public static List<PathRule> genPathRules(String id, List<Map<String, Object>> ruleConfigs) {
        List<PathRule> rules = new ArrayList<PathRule>();
        for (Map<String, Object> ruleConfig : ruleConfigs) {
            rules.add(genPathRule(id, ruleConfig));
        }
        return rules;
    }

    @SuppressWarnings("unchecked")
    public static PathRule genPathRule(String id, Map<String, Object> ruleConfig) {
        PathRule p = new PathRule();
        if (!ruleConfig.containsKey("pathID")) {
            fail(id + ": Invalid config, rule must have pathID");
        }

        if (!ruleConfig.containsKey("priority")) {
            fail(id + ": Invalid config, rule must have priority");
        }

        if (!ruleConfig.containsKey("effectOwner")) {
            fail(id + ": Invalid config, rule must have effect_owner");
        }

        if (!ruleConfig.containsKey("nodes")) {
            fail(id + ": Invalid config, rule must have nodes");
        }

        p.path_id = (Integer) ruleConfig.get("pathID");
        p.priority = (Integer) ruleConfig.get("priority");
        p.effect_owner = (Integer) ruleConfig.get("effectOwner");

        List<Map<String, Object>> path = (List<Map<String, Object>>) ruleConfig.get("nodes");
        assertNotNull(id + ": config error, path nodes is null", path);
        p.node = genPath(id, path);

        return p;
    }

    @SuppressWarnings("unchecked")
    public static ArrayList<PathNode> genPath(String id, List<Map<String, Object>> pathConfig) {
        ArrayList<PathNode> nodes = new ArrayList<PathNode>();
        for (Map<String, Object> node : pathConfig) {
            if (!node.containsKey("id")) {
                fail(id + ": Invalid config, rule.nodes must have id");
            }
            if (!node.containsKey("type_refs")) {
                fail(id + ": Invalid config, rule.nodes must have type_refs");
            }
            if (!node.containsKey("next")) {
                fail(id + ": Invalid config, rule.nodes must have next");
            }
            if (!node.containsKey("expand")) {
                fail(id + ": Invalid config, rule.nodes must have expand");
            }

            PathNode p = new PathNode();
            p.id = (Integer) node.get("id");

            List<Integer> types = (List<Integer>) node.get("type_refs");
            assertTrue(types.size() > 0);
            p.type_refs = new int[types.size()];
            int i = 0;
            for (int type : types) {
                p.type_refs[i] = type;
                i++;
            }
            p.next = (Integer) node.get("next");
            p.expand = (String) node.get("expand");
            nodes.add(p);
        }

        return nodes;
    }

    /**
     * 自定义 yaml 字符串序列化实现，避免默认实现将来源标识表达为 Base64 编码串
     * 
     * @author wxz
     * 
     */
    protected static class MyRepresenter extends Representer {
        public MyRepresenter() {
            this.representers.put(String.class, new RepresentString());
        }

        private class RepresentString implements Represent {
            @Override
            public Node representData(Object data) {
                Tag tag = Tag.STR;
                String value = data.toString();
                return representScalar(tag, value);
            }
        }
    }

}
