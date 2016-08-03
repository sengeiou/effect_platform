package com.ali.lz.effect.holotree;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ali.lz.effect.rule.RuleSet;
import com.ali.lz.effect.utils.StringPool;

/**
 * 全息树构建器
 * 
 * @author wxz
 * 
 */
public class HoloTreeBuilder {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(HoloTreeBuilder.class);

    private static final String OTHER_SRC_PATH_FEATURE = "-1\002";

    private static final String LOGIN_REDIRECT_URL_PREFIX = "https://login.taobao.com/member/";
    private static final String[] LOGIN_REDIRECT_URL_KEYS = { "redirectURL", "redirect_url", "tpl_redirect_url",
            "redirectUrl", "TPL_redirect_url" };

    // 持有配置对象引用
    private HoloConfig conf;
    // 单个session 内允许用来建树的最大节点数，超过此数量时将会强行截断为多棵树
    private int maxSessNodes;
    // 是否在完成单个树的构建过程后自动进行删减压缩
    private boolean autoCompact;
    // 当前session ID
    private String curSid;
    // 将要处理的日志记录对应当前session 内的序号，session内首条日志的序号为0，往后按条递增
    private int curSessSerial;
    // 当前正在构建的全息树列表，按根节点出现时间从先到后排列
    private List<HoloTree> curTrees = new LinkedList<HoloTree>();

    // 当前构建的所有树中url同节点的对应关系，用于快速查找父节点，url 相同时后来的节点引用会覆盖之前的
    private Map<Integer, HoloTreeNode> curUrlToNode = new HashMap<Integer, HoloTreeNode>();
    // 当前构建的所有树中淘宝登录页重定向目标url同登录页节点的对应关系，用于修复因登录造成的refer丢失
    private Map<Integer, HoloTreeNode> curLoginRedirects = new HashMap<Integer, HoloTreeNode>();

    // 来源路径规则引用
    private RuleSet srcRuleSet;
    // 来源路径规则中所有效果页类型集合，用于优化匹配调用及识别其他来源
    private Set<Integer> srcEPTypeSet;

    // 建树事件处理器列表
    private List<HoloTreeProcessor> procs = new ArrayList<HoloTreeProcessor>();

    // 路径匹配染色标记
    private boolean doPathMatch = true;

    public HoloTreeBuilder(HoloConfig conf) {
        this(conf, false, null);
    }

    /**
     * 全息树构建器构造函数
     * 
     * @param conf
     *            配置对象
     * @param autoCompact
     *            是否在完成单个全息树构建后自动进行删减压缩, 实时计算时需开启, 离线无需开启
     * @param procs
     *            建树事件处理器
     */
    public HoloTreeBuilder(HoloConfig conf, boolean autoCompact, List<HoloTreeProcessor> procs) {
        this.conf = conf;
        this.autoCompact = autoCompact;
        this.maxSessNodes = conf.getMaxSessionNodes();
        this.doPathMatch = conf.doPathMatch;
        this.srcRuleSet = new RuleSet(conf);
        this.srcEPTypeSet = srcRuleSet.getEffectPageSet();
        if (procs != null) {
            this.procs.addAll(procs);
        }

        initialize();
    }

    /**
     * 设置路径匹配染色标记
     * 
     * @param flag
     *            为 true 表示进行路径匹配染色，false 表示不进行该操作
     */
    public void setDoPathMatch(boolean flag) {
        doPathMatch = flag;
    }

    public HoloConfig getHoloConfig() {
        return this.conf;
    }

    /**
     * 传入构建全息树所需日志, 建树时会根据 HoloConfig 中定义的每 session 最大节点数自动截断为多棵树
     * 
     * <b>注意：传入的日志应全部来自同一用户！</b>
     * 
     * <b>目前假定同一用户的多个 session 数据是顺序聚集到达的，即同一 session 的数据连续到达，出现新 session 时隐含着原
     * session 内所有全息树都完成了；若用户开启多个浏览器实例（非多标签），则可能出现 session 数据交错的情况，这时可能原本属于同一
     * session 内的数据可能被切分到多个树上，此情况应该不多，现在不予考虑。</b>
     * 
     * @param logEntry
     *            扩充日志记录
     */
    public void appendLog(PTLogEntry logEntry) {
        // 0. 修正 atpanel 日志中的 refer 和 url，解码 \\xUU 和 \\uUUUU 这样的转义序列
        fixupReferAndUrl(logEntry);

        // 1. session发生变化或当前session节点数超过最大限制时自动截断之前所有树
        if (needCutOff(logEntry)) {
            clearCache();

            // CB: 调用建树完成回调接口
            for (HoloTree tree : curTrees) {
                HoloTreeNode lastNode = tree.get(tree.lastKey());
                for (HoloTreeProcessor proc : procs) {
                    proc.onCompleteTree(conf, lastNode);
                }
            }

            if (autoCompact) {
                curTrees.clear();
            }
        }

        HoloTreeNode curNode = new HoloTreeNode(logEntry);

        // 2. 尝试用日志记录中原始 refer 关联到当前某棵树上
        // 3. 若无法用原始 refer 直接关联到树上，尝试通过修正后 refer 关联
        HoloTreeNode prevNode = attachNodeByRefer(curNode);
        if (prevNode == null) {
            // 4a. 用修正后 refer 仍无法关联，当前日志成为新的树根
            HoloTree curTree = new HoloTreeImpl();

            processRootNode(curNode, curTree);

            // 将新树加入当前构建树列表
            curTrees.add(curTree);

            // CB: 调用新建树回调函数
            for (HoloTreeProcessor proc : procs) {
                proc.onNewTree(conf, curNode);
            }
        } else {
            // 4b. 可以用原始 refer 直接关联到树上，当前日志成为中间节点
            HoloTree curTree = prevNode.getTree();
            processInternalNode(prevNode, curNode, curTree);
        }

        // 5. 对 curNode 的页面类型 root path 进行来源路径匹配识别并对其染色
        if (doPathMatch) {
            srcPathMatch(curNode);

            if (curNode.getSources().size() > 0) {
                // CB: 调用已染色页回调函数
                for (HoloTreeProcessor proc : procs) {
                    proc.onColoredNode(conf, curNode);
                }
            }

            if (curNode.isEffectPage()) {
                // CB: 调用效果页回调函数
                for (HoloTreeProcessor proc : procs) {
                    proc.onEffectPage(conf, curNode);
                }
            }

            HoloTreeNode parentNode = curNode.getParent();
            if (parentNode != null && parentNode.isEffectPage()) {
                // CB: 调用效果页下一跳回调函数
                for (HoloTreeProcessor proc : procs) {
                    proc.onEffectPageChild(conf, curNode);
                }
            }
        }

        // 缓存必要信息以便加速建树操作
        cacheNode(curNode);

        // 增加 session 内节点序号
        curSessSerial++;
    }

    /**
     * 获取当前全息树列表, 每棵树为按时间排序的节点集合, 树结构通过 HoloTreeNode 相关方法获取. 仅离线计算使用
     * 
     * @return 当前全息树列表, 按根节点出现时间从旧到新排列
     */
    public List<HoloTree> getCurrentTrees() {
        return curTrees;
    }

    /**
     * 清空当前全息树数据, 仅离线计算使用
     */
    public void flush() {
        initialize();
    }

    private void initialize() {
        curSid = "";
        curSessSerial = 0;

        curTrees.clear();
        clearCache();
    }

    /**
     * 尝试修复由于登录、广告跳转等因素引起的 refer 变化，并用修正后的 refer 查找全息树父节点
     * 
     * @param node
     * @return
     */
    private HoloTreeNode attachNodeByRefer(HoloTreeNode node) {
        PTLogEntry logEntry = node.getPtLogEntry();

        String refer = (String) logEntry.get("refer_url");
        HoloTreeNode prevNode = curUrlToNode.get(refer.hashCode());
        if (prevNode == null) {
            // 尝试修复登录引起的refer丢失
            adjustReferByLoginNode(node);
        } else {
            return prevNode;
        }

        refer = (String) logEntry.get("refer_url");
        prevNode = curUrlToNode.get(refer.hashCode());
        if (prevNode == null) {
            // 尝试修复广告跳转引起的refer丢失
            adjustReferByAdNode(node);
        } else {
            return prevNode;
        }

        refer = (String) logEntry.get("refer_url");
        prevNode = curUrlToNode.get(refer.hashCode());
        return prevNode;
    }

    private void clearCache() {
        curUrlToNode.clear();
        curLoginRedirects.clear();
    }

    private void cacheNode(HoloTreeNode curNode) {
        PTLogEntry logEntry = curNode.getPtLogEntry();
        String url = (String) logEntry.get("url");

        // 添加当前日志url和全息树节点关系到查找映射表中
        curUrlToNode.put(url.hashCode(), curNode);

        // 如果当前节点是登录节点，则把登录之后跳转的URL作为key当前节点作为value保存起来
        // 用来判断一个没有refer的节点是否是因为登录而丢失了referer
        if (url.startsWith(LOGIN_REDIRECT_URL_PREFIX)) {
            String[] queryItems = url.split("\\?");
            if (queryItems.length == 1) {
                return;
            }
            String query = queryItems[1];
            Map<String, String> queryMap = HoloTreeUtil.splitStr(query, "&", "=");
            for (String redirectKey : LOGIN_REDIRECT_URL_KEYS) {
                if (queryMap.containsKey(redirectKey)) {
                    try {
                        String redirectURL = URLDecoder.decode(queryMap.get(redirectKey), "utf-8");
                        String path = redirectURL.split("\\?")[0]; // 忽略后面的参数
                        curLoginRedirects.put(path.hashCode(), curNode);
                        break; // 多个redirctKey不会同时存在
                    } catch (Exception e) {
                    }
                }
            }
        }
    }

    /**
     * 执行来源路径匹配识别并对当前节点进行染色
     * 
     * @param curNode
     */
    private void srcPathMatch(HoloTreeNode curNode) {
        PTLogEntry logEntry = curNode.getPtLogEntry();
        int pType = logEntry.getPType();
        int rType = logEntry.getRType();
        curNode.setEffectPage(false);

        boolean exactPTypeMatch = srcEPTypeSet.contains(pType);
        boolean exactRTypeMatch = srcEPTypeSet.contains(rType);

        /*
         * 优化：仅当curNode的pType或rType在用户定义路径效果页类型集合中存在时才执行匹配操作
         */
        if (exactPTypeMatch || exactRTypeMatch) {
            // “其他来源”匹配识别
            if (exactPTypeMatch && pType != 0) {
                // 当前页面不是未知类型，应该标记为效果页
                curNode.setEffectPage(true);
                // 识别出了“其他来源”
                curNode.addSource(OTHER_SRC_PATH_FEATURE, new SourceMeta(curNode));
            }

            boolean needMore = true;
            HoloTreeNode node = curNode;
            while (needMore) {
                needMore = srcRuleSet.matchNext(node);
                if (node != null) {
                    node = node.getParent();
                }
            }

            srcRuleSet.reset();
        }
    }

    /**
     * 将当前节点处理为全息树中间节点，并将其关联到树上
     * 
     * @param prevNode
     * @param curNode
     * @param curTree
     */
    private void processInternalNode(HoloTreeNode prevNode, HoloTreeNode curNode, HoloTree curTree) {
        PTLogEntry logEntry = curNode.getPtLogEntry();
        long ts = (Long) logEntry.get("ts");
        // 将日志中的秒级时戳同session 内节点序号整合，以避免时戳重复影响建树
        long uniqTS = ts * maxSessNodes + curSessSerial;
        int rType = logEntry.getRType();
        int pType = logEntry.getPType();

        curNode.setParent(prevNode);
        prevNode.getChildren().add(curNode);
        curNode.setUniqTS(uniqTS);

        // 继承父节点的来源染色
        curNode.inheritSources();

        // 设置序号root path
        curNode.setSerialRootPath(prevNode.getSerialRootPath() + "." + String.valueOf(curSessSerial));

        // 设置页面类型root path
        int prevPType = prevNode.getPtLogEntry().getPType();
        String ptRootPath = prevNode.getPTypeRootPath();
        // 处理可能出现的页面类型歧义
        if (rType != 0 && rType != prevPType) {
            // 需要用当前节点的rType 替换root path 中的父节点页面类型
            ptRootPath = HoloTreeUtil.replacePTRootPathLast(ptRootPath, rType);
        }
        String pTypeStr = String.valueOf(HoloTreeUtil.pageTypeIdToChar(pType));
        ptRootPath = ptRootPath + pTypeStr;
        curNode.setPTypeRootPath(ptRootPath);

        // 当前节点插入父节点所在树
        curTree.put(uniqTS, curNode);
        curNode.setTree(curTree);

        // 计算页面停留时间
        if (prevNode.getPtLogEntry().containsKey("page_duration") && logEntry.containsKey("page_duration")) {
            long pageDuration = (Long) logEntry.get("ts") - (Long) prevNode.getPtLogEntry().get("ts");
            if (pageDuration > (Long) prevNode.getPtLogEntry().get("page_duration")) {
                prevNode.getPtLogEntry().put("page_duration", pageDuration);
            }
        }
    }

    /**
     * 将给定节点处理为新树的根节点，并将其关联到树上
     * 
     * @param curNode
     * @param curTree
     */
    private void processRootNode(HoloTreeNode curNode, HoloTree curTree) {
        PTLogEntry logEntry = curNode.getPtLogEntry();
        long ts = (Long) logEntry.get("ts");
        // 将日志中的秒级时戳同session 内节点序号整合，以避免时戳重复影响建树
        long uniqTS = ts * maxSessNodes + curSessSerial;
        int rType = logEntry.getRType();
        int pType = logEntry.getPType();

        curNode.setParent(null);
        curNode.setUniqTS(uniqTS);

        // 设置序号root path，树根节点的root path 只有一个字段
        curNode.setSerialRootPath(String.valueOf(curSessSerial));

        // 设置页面类型root path
        String rTypeStr = String.valueOf(HoloTreeUtil.pageTypeIdToChar(rType));
        String pTypeStr = String.valueOf(HoloTreeUtil.pageTypeIdToChar(pType));
        String ptRootPath = rTypeStr + pTypeStr;
        curNode.setPTypeRootPath(ptRootPath);

        // 将当前节点插入新树
        curTree.put(uniqTS, curNode);
        curNode.setTree(curTree);
    }

    /**
     * 判断全息树截断条件是否满足
     * 
     * @param logEntry
     * @return
     */
    private boolean needCutOff(PTLogEntry logEntry) {
        boolean cutOff = false;
        String newSid = (String) logEntry.get("sid");

        if (!curSid.equalsIgnoreCase(newSid)) {
            // 发现新 session
            curSid = newSid;
            curSessSerial = 0;
            cutOff = true;
        } else {
            if (curSessSerial > maxSessNodes - 1) {
                curSessSerial = 0;
                cutOff = true;
            }
        }

        return cutOff;
    }

    // 调整login跳转之后的referer，login跳转之后referer为空
    // login跳转的URL为https://login.taobao.com/member/login.jhtml?redirectURL=http%3A%2F%2Fi.taobao.com%2Fmy_taobao.htm
    // 跳转之后的URL为redirectURL的参数。我们在处理login节点的时候，会保存redirectURL到login节点的映射
    // 因此处理登录之后的节点，如果没有referer，并且URL存在于login节点的映射表中，我们就认为它的父节点为该longin节点
    private void adjustReferByLoginNode(HoloTreeNode curNode) {
        PTLogEntry logEntry = curNode.getPtLogEntry();
        String url = (String) logEntry.get("url");

        // 由于login登录之后会加上一些参数，所以我们只取问号之前的部分来处理，避免添加的参数对我们查找造成的影响
        String path = url.split("\\?", -1)[0];
        Integer pathHash = path.hashCode();

        HoloTreeNode loginNode = curLoginRedirects.get(pathHash);
        if (loginNode != null) {
            String loginUrl = (String) loginNode.getPtLogEntry().get("url");
            // 更新当前节点的 refer
            logEntry.put("refer_url", loginUrl);
        }
    }

    // 调整广告请求的referer，某些广告请求的referer需要从URL中作为一个参数被取出来
    private void adjustReferByAdNode(HoloTreeNode node) {
        PTLogEntry logEntry = node.getPtLogEntry();
        // 处理需要未unescape的原始refer
        String rawRefer = (String) logEntry.get("raw_refer_url");

        // 从URL中获取真实的referer
        String trueReferer = HoloTreeUtil.getTrueReferer(rawRefer);

        // 设置当前节点的referer为真实的referer
        if (trueReferer != null && !trueReferer.equals("")) {
            logEntry.put("refer_url", trueReferer);
        }
    }

    /**
     * 修正 atpanel 日志的 refer 和 url，解开 \\xUU 和 \\uUUUU 形式的转义序列
     * 
     * @param logEntry
     */
    private void fixupReferAndUrl(PTLogEntry logEntry) {
        String refer = (String) logEntry.get("refer_url");
        String url = (String) logEntry.get("url");

        String fixedRefer = HoloTreeUtil.unescape(refer);
        String fixedUrl = HoloTreeUtil.unescape(HoloTreeUtil.transformUrl(url));

        // 若是实时应用场景，则将原始日志中的refer/url字符串去重后改为保存唯一对象引用
        if (autoCompact) {
            fixedRefer = StringPool.poolStr(fixedRefer);
            fixedUrl = StringPool.poolStr(fixedUrl);
            refer = StringPool.poolStr(refer);
        }

        logEntry.put("raw_refer_url", refer);
        logEntry.put("refer_url", fixedRefer);
        logEntry.put("url", fixedUrl);
    }

}
