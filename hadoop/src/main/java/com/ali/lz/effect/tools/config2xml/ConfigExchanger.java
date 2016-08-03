/**
 * 
 */
package com.ali.lz.effect.tools.config2xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Element;

import com.ali.lz.effect.tools.util.ConfigLoader;
import com.ali.lz.effect.tools.util.Constants;
import com.ali.lz.effect.tools.util.JDBCUtil;
import com.ali.lz.effect.tools.util.TimeUtil;

/**
 * 从MySQL中同步用户配置生成XML文件
 * 
 * @author jiuling.ypf
 * 
 */
public class ConfigExchanger {

    // 日志操作记录对象
    private static final Log LOG = LogFactory.getLog(ConfigExchanger.class);

    // MySQL数据库操作对象
    private MySQLManager effectDbMgr = null;

    /**
     * 构造函数
     */
    public ConfigExchanger(MySQLManager dbMgr) {
        // TODO Auto-generated constructor stub
        this.effectDbMgr = dbMgr;
    }

    /**
     * 主处理逻辑
     */
    public void exchangeConfig() {
        long startTime = System.currentTimeMillis();
        LOG.info("begin to exchange config from MySQL to XML");

        // 0. 查询plan_share表得到所有plan_id，然后根据plan_id查询plan_config表得到计划配置信息
        List<PlanShareResult> planShareList = effectDbMgr.queryPlanShare();

        // 1. 处理过期的plan_id，为离线报表生成过期XML文件，为实时报表杀掉storm任务
        for (int i = 0; i < planShareList.size(); i++) {
            PlanShareResult planShareRecord = (PlanShareResult) planShareList.get(i);
            if (planShareRecord.getType() == Constants.PLAN_SHARE_TYPE_FORBIDDEN) {
                dealWithOverduePlan(planShareRecord.getPlanId());
            }
        }

        List<PlanConfigResult> planConfigList = effectDbMgr.queryPlanConfig(planShareList);

        // 2. 依次遍历各个plan_id的计划配置信息
        for (int i = 0; i < planConfigList.size(); i++) {
            PlanConfigResult planConfig = (PlanConfigResult) planConfigList.get(i);
            int planId = planConfig.getPlanId();
            int userId = planConfig.getUserId();
            String effectUrl = planConfig.getEffectUrl();
            String srcType = planConfig.getSrcType();
            int pathType = planConfig.getPathType();
            int belongId = planConfig.getBelongId();
            String indIds = planConfig.getIndIds();
            int period = planConfig.getPeriod();
            int expireType = planConfig.getExpireType();
            String linkType = planConfig.getLinkType();

            // 2.1 为plan_id创建XML根节点
            XMLGenerator xmlGen = new XMLGenerator(planId);
            Element rootNode = xmlGen.createElement(null, "effect_plan");

            // 2.2 生成XML不变配置部分
            xmlGen.createElement(rootNode, "ver", String.valueOf(EffectPlanConfig.VER));
            xmlGen.createElement(rootNode, "update_interval", String.valueOf(EffectPlanConfig.UPDATE_INTERVAL));
            List<KeyValuePair> attrList = new ArrayList<KeyValuePair>();
            attrList.add(new KeyValuePair("method", EffectPlanConfig.TREE_SPLIT));
            xmlGen.createElement(rootNode, "tree_split", attrList);

            // 2.3 根据plan_config表中内容生成基本配置
            xmlGen.createElement(rootNode, "analyzer_id", String.valueOf(userId));
            xmlGen.createElement(rootNode, "plan_id", String.valueOf(planId));
            if (expireType != 1) { // TODO 目前前端默认为1，否则报错
                LOG.error("plan_id " + planId + " has invalid expire_type value in plan_config table");
                continue;
            }
            xmlGen.createElement(rootNode, "ttl", "90");
            xmlGen.createElement(rootNode, "period", String.valueOf(period));
            if (linkType == null || linkType.equals("")) // TODO 不指定时默认为
                // 1，即仅查看效果页本身和其下一跳
                linkType = "1";
            xmlGen.createElement(rootNode, "lookahead", linkType);
            attrList.clear();
            if (belongId < 1 || belongId > EffectPlanConfig.ATTR_CALC.length) {
                LOG.error("plan_id " + planId + " has invalid belong_id found: " + belongId);
                continue;
            }
            attrList.add(new KeyValuePair("method", EffectPlanConfig.ATTR_CALC[belongId - 1]));
            xmlGen.createElement(rootNode, "attr_calc", attrList);

            // 2.4 根据plan_config表中ind_ids字段内容生成指标配置
            if (indIds == null) { // 非法的planId不再创建xml
                LOG.error("plan_id " + planId + " has null ind_ids, terminate to create its xml file");
                continue;
            }
            String[] indIdItems = indIds.split(",");
            boolean isMidPageInd = false;
            Element effectsNode = xmlGen.createElement(rootNode, "effects");
            int IndIndex = 0;
            for (String indIdItem : indIdItems) { // 遍历每个plan_id下的各个ind_ids
                attrList.clear();
                attrList.add(new KeyValuePair("id", String.valueOf(IndIndex++)));
                attrList.add(new KeyValuePair("ind_id", indIdItem));
                xmlGen.createElement(effectsNode, "ind", attrList);
                if (Integer.parseInt(indIdItem) >= 400) { // 包含中间页计算指标
                    isMidPageInd = true;
                }
            }

            // 2.5 创建XML的URL规则部分的根节点
            Element urlTypeNode = xmlGen.createElement(rootNode, "url_type");

            // 2.6 为src_type中出现的预设类型生成URL类型匹配配置
            if (srcType == null || srcType.equals("0")) { // TODO 暂时不支持不限定来源功能
                LOG.error("plan_id " + planId + " has invalid src_type in plan_config table");
                continue;
            }
            String[] srcTypeItems = srcType.split(",");
            StringBuffer sb = new StringBuffer();
            boolean isOuterAd = false;
            boolean isSPMAd = false;
            boolean first = true;
            for (String srcTypeItem : srcTypeItems) {
                if (srcTypeItem.equals("100")) { // 外投广告
                    isOuterAd = true;
                } else if (srcTypeItem.equals("103")) { // SPM广告
                    isSPMAd = true;
                } else {
                    if (first) {
                        sb.append(srcTypeItem);
                        first = false;
                    } else {
                        sb.append("," + srcTypeItem);
                    }
                }
                int defaultUrlIndex = Integer.parseInt(srcTypeItem) - 100;
                if (defaultUrlIndex >= UrlTypeConfig.DEFAULT_IDS.length) { // 非法的url_type
                    LOG.error("invalid default url_type id: " + srcTypeItem);
                }
                Element ruleNode = xmlGen.createElement(urlTypeNode, "rule");
                xmlGen.createElement(ruleNode, "priority", String.valueOf(UrlTypeConfig.PRIORITY));
                xmlGen.createElement(ruleNode, "type_id", srcTypeItem);
                xmlGen.createElement(ruleNode, "match_field", UrlTypeConfig.DEFAULT_MATCH_FIELDS[defaultUrlIndex]);
                xmlGen.createElement(ruleNode, "target_type", UrlTypeConfig.DEFAULT_TARGET_TYPES[defaultUrlIndex]);
                Element matchRegexpsNode = xmlGen.createElement(ruleNode, "match_regexps");
                Element matchRegexpNode = xmlGen.createElement(matchRegexpsNode, "match_regexp");
                xmlGen.createElementCDATAValue(matchRegexpNode, "regexp",
                        UrlTypeConfig.DEFAULT_MATCH_REGEXPS[defaultUrlIndex]);
                if (!UrlTypeConfig.DEFAULT_MATCH_PROP_FIELDS[defaultUrlIndex].equals("")) {
                    Element propsMatchNode = xmlGen.createElement(matchRegexpNode, "props");
                    attrList.clear();
                    attrList.add(new KeyValuePair("field", UrlTypeConfig.DEFAULT_MATCH_PROP_FIELDS[defaultUrlIndex]));
                    attrList.add(new KeyValuePair("value", UrlTypeConfig.DEFAULT_MATCH_PROP_VALUES[defaultUrlIndex]));
                    xmlGen.createElement(propsMatchNode, "prop", attrList);
                }
                if (!UrlTypeConfig.DEFAULT_EXTRACT_REGEXPS[defaultUrlIndex].equals("")) {
                    Element extractRegexpsNode = xmlGen.createElement(ruleNode, "extract_regexps");
                    Element extractRegexpNode = xmlGen.createElement(extractRegexpsNode, "extract_regexp");
                    xmlGen.createElementCDATAValue(extractRegexpNode, "regexp",
                            UrlTypeConfig.DEFAULT_EXTRACT_REGEXPS[defaultUrlIndex]);
                    if (!UrlTypeConfig.DEFAULT_EXTRACT_PROP_FIELDS[defaultUrlIndex].equals("")) {
                        Element propsExtractNode = xmlGen.createElement(extractRegexpNode, "props");
                        attrList.clear();
                        attrList.add(new KeyValuePair("field",
                                UrlTypeConfig.DEFAULT_EXTRACT_PROP_FIELDS[defaultUrlIndex]));
                        attrList.add(new KeyValuePair("value",
                                UrlTypeConfig.DEFAULT_EXTRACT_PROP_VALUES[defaultUrlIndex]));
                        xmlGen.createElement(propsExtractNode, "prop", attrList);
                    }
                }
            }

            // 辅助变量，用于记录srcType中除了100和103外的来源ID（逗号分隔）
            String srcTypeCommon = sb.toString();

            // 辅助变量，用于存储effect_url和path_config中url的自定义type_id值
            Map<String, Integer> url2TypeIdMap = new HashMap<String, Integer>();

            // 2.7 取出plan_config表中的effect_url，为其分配唯一且大于等于10000 的页面类型ID：10000
            // 然后为效果页effect_url生成URL类型匹配配置
            if (effectUrl == null) { // 非法的planId不再创建xml
                LOG.error("plan_id " + planId + " has null effect_url, terminate to create its xml file");
                continue;
            }
            int userDefUrlTypeId = UrlTypeConfig.TYPE_ID;
            Element ruleNNode = xmlGen.createElement(urlTypeNode, "rule");
            xmlGen.createElement(ruleNNode, "priority", String.valueOf(UrlTypeConfig.PRIORITY));
            url2TypeIdMap.put(effectUrl, userDefUrlTypeId);
            xmlGen.createElement(ruleNNode, "type_id", String.valueOf(userDefUrlTypeId++));
            xmlGen.createElement(ruleNNode, "match_field", "url"); // v1.0版本中为用户自定义URL生成的匹配规则总是(url,url)形式
            xmlGen.createElement(ruleNNode, "target_type", "url");
            Element matchRegexpsNode = xmlGen.createElement(ruleNNode, "match_regexps");
            Element matchRegexpNode = xmlGen.createElement(matchRegexpsNode, "match_regexp");
            xmlGen.createElementCDATAValue(matchRegexpNode, "regexp", "^" + Pattern.quote(effectUrl));

            // 辅助变量，用于存储path_id对应的data信息
            Map<Integer, List<PathDataRecord>> pathId2DataMap = new HashMap<Integer, List<PathDataRecord>>();

            // 2.8
            // 从path_config表中根据plan_id查找所有path_id，然后为各个path_id中的URL分别生成URL类型匹配配置
            List<Integer> pathIdList = new ArrayList<Integer>();
            if (pathType == 1) { // path_type=1时查询path_config表
                pathIdList = effectDbMgr.queryPathId(planId);
                if (pathIdList.size() == 0) { // path_type=1但实际没有用户设置的路径时，将path_type重置为0
                    pathType = 0;
                } else { // 为用户设置的路径规则生成url匹配信息
                    for (int pathIdIndex = 0; pathIdIndex < pathIdList.size(); pathIdIndex++) {
                        // 根据path_id，查找data字段后解析JSON格式
                        int pathId = pathIdList.get(pathIdIndex).intValue();
                        String pathData = effectDbMgr.queryPathConfig(pathId);
                        List<PathDataRecord> pathDataRecords = JsonUtil.fromJson(pathData);
                        pathId2DataMap.put(Integer.valueOf(pathId), pathDataRecords);
                        // 依次遍历当前path_id对应path中的各个url为其分配type_id
                        for (PathDataRecord dataRecord : pathDataRecords) {
                            if (dataRecord.getUrl() == null) { // 非法的planId不再创建xml
                                LOG.error("plan_id " + planId + " has null path url, terminate to create its xml file");
                                continue;
                            }
                            if (url2TypeIdMap.containsKey(dataRecord.getUrl())) // 如果dataRecord.getUrl()已经存在，则不再重复进行编号
                                continue;
                            Element ruleMNode = xmlGen.createElement(urlTypeNode, "rule");
                            xmlGen.createElement(ruleMNode, "priority", String.valueOf(UrlTypeConfig.PRIORITY));
                            url2TypeIdMap.put(dataRecord.getUrl(), userDefUrlTypeId);
                            xmlGen.createElement(ruleMNode, "type_id", String.valueOf(userDefUrlTypeId++));
                            xmlGen.createElement(ruleMNode, "match_field", "url"); // v1.0版本中为用户自定义URL生成的匹配规则总是(url,url)形式
                            xmlGen.createElement(ruleMNode, "target_type", "url");
                            Element matchRegexpsMNode = xmlGen.createElement(ruleMNode, "match_regexps");
                            Element matchRegexpMNode = xmlGen.createElement(matchRegexpsMNode, "match_regexp");
                            xmlGen.createElementCDATAValue(matchRegexpMNode, "regexp",
                                    "^" + Pattern.quote(dataRecord.getUrl()));
                        }
                    }
                }
            }

            // 2.9 创建XML的path规则部分的根节点
            Element srcPathNode = xmlGen.createElement(rootNode, "src_path");

            // 2.10 根据是否有用户自定义路径，生成路径规则配置
            if (pathType == 0) { // 两种情况下生成默认路径：1）用户未设置路径限定规则，2）用户虽然设置了但是却没有指定具体路径规则；此时只生成一条默认路径ID为0的路径：<src_types>-1-><effect_url>
                // 生成一般来源路径规则
                generateSrcPath(xmlGen, srcPathNode, srcTypeCommon, effectUrl, url2TypeIdMap, isMidPageInd,
                        SrcPathConfig.DEFAULT_PATH_ID_BASE);
                if (isOuterAd) { // 生成外投广告路径规则
                    generateSrcPath(xmlGen, srcPathNode, srcTypeCommon, effectUrl, url2TypeIdMap, isMidPageInd,
                            SrcPathConfig.OUTER_ADS_PATH_ID_BASE);
                }
                if (isSPMAd) { // 生成SPM广告路径规则
                    generateSrcPath(xmlGen, srcPathNode, srcTypeCommon, effectUrl, url2TypeIdMap, isMidPageInd,
                            SrcPathConfig.SPM_ADS_PATH_ID_BASE);
                }
            } else if (pathType == 1) { // 设定了路径限定规则，依次遍历各个路径生成路径配置
                // 生成一般来源路径规则
                generateSrcPath(xmlGen, srcPathNode, srcTypeCommon, effectUrl, pathIdList, pathId2DataMap,
                        url2TypeIdMap, isMidPageInd, SrcPathConfig.DEFAULT_PATH_ID_BASE);
                if (isOuterAd) { // 生成外投广告路径规则
                    generateSrcPath(xmlGen, srcPathNode, srcTypeCommon, effectUrl, pathIdList, pathId2DataMap,
                            url2TypeIdMap, isMidPageInd, SrcPathConfig.OUTER_ADS_PATH_ID_BASE);
                }
                if (isSPMAd) { // 生成SPM广告路径规则
                    generateSrcPath(xmlGen, srcPathNode, srcTypeCommon, effectUrl, pathIdList, pathId2DataMap,
                            url2TypeIdMap, isMidPageInd, SrcPathConfig.SPM_ADS_PATH_ID_BASE);
                }
            }

            // 2.11 输出生成XML配置文件
            boolean flag = xmlGen.outputXMLFile();

            // 2.12 为离线和实时报表完成后续处理
            if (flag)
                dealWithFollowWork(planId);
        }

        // 3. 清空plan_shared表中记录
        boolean flag = effectDbMgr.deletePlanShare(planShareList);
        if (!flag) {
            LOG.error("failed to delete plan_share, but xml files has been created");
        } else {
            LOG.info("finish to exchange config from MySQL to XML, total time: "
                    + (System.currentTimeMillis() - startTime) + " ms");
        }
    }

    /**
     * 为离线和实时分别做后续处理操作
     * 
     * @param planId
     */
    private void dealWithFollowWork(int planId) {
        // XXX: 离线报表复制配置文件
        String outputDirPrefix = ConfigLoader.getXmlOutputDirPrefix();
        String outputFileFormat = ConfigLoader.getXmlOutputFileFormat();
        String[] outputFileTokens = outputFileFormat.split("\\*");
        String outputFilePrefix = "report_";
        String outputFileSuffix = ".xml";
        if (outputFileTokens.length == 2) {
            outputFilePrefix = outputFileTokens[0];
            outputFileSuffix = outputFileTokens[1];
        }
        String offlineDirPrefix = ConfigLoader.getXmlOfflineDirPrefix();
        String offlineFileFormat = ConfigLoader.getXmlOfflineFileFormat();
        String[] offlineFileTokens = offlineFileFormat.split("\\*");
        String offlineFilePrefix = "report_";
        String offlineFileSuffix = ".xml";
        if (offlineFileTokens.length == 2) {
            offlineFilePrefix = offlineFileTokens[0];
            offlineFileSuffix = offlineFileTokens[1];
        }

        String srcDir = outputDirPrefix + TimeUtil.getTodayDir() + "/";
        String srcFileName = outputFilePrefix + planId + outputFileSuffix;
        String destDir = offlineDirPrefix + TimeUtil.getTodayDir() + "/";
        String destFileName = offlineFilePrefix + planId + offlineFileSuffix;
        File srcFile = new File(srcDir + srcFileName);
        if (srcFile.exists()) {
            try {
                File destFile = new File(destDir, destFileName);
                File parentFile = destFile.getParentFile();
                if (!parentFile.exists()) // 父目录不存在则首先进行创建
                    parentFile.mkdirs();
                FileChannel fcin = new FileInputStream(srcFile).getChannel();
                FileChannel fcout = new FileOutputStream(destFile).getChannel();
                fcin.transferTo(0, fcin.size(), fcout);
                fcin.close();
                fcout.close();
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
            LOG.info("transfer offline xml file for plan_id: " + planId + " to directory: " + destDir);
        }
        // XXX: 实时报表提交Storm任务
        // String shellScript =
        // ClassLoader.getSystemResource("storm-jar.sh").getFile();
        // OuterExeProcess.execute("/bin/bash " + shellScript + " " + planId);
    }

    /**
     * 为离线和实时分别处理过期的报表
     * 
     * @param planId
     */
    private void dealWithOverduePlan(int planId) {
        LOG.warn("plan_id: " + planId + " is forbidden in plan_share table");
        // XXX: 为离线报表生成过期XML文件
        String overdueDirPrefix = ConfigLoader.getXmlOverdueDirPrefix();
        String overdueFileFormat = ConfigLoader.getXmlOverdueFileFormat();
        String[] overdueFileTokens = overdueFileFormat.split("\\*");
        String overdueFilePrefix = "report_";
        String overdueFileSuffix = ".xml";
        if (overdueFileTokens.length == 2) {
            overdueFilePrefix = overdueFileTokens[0];
            overdueFileSuffix = overdueFileTokens[1];
        }
        String overdueDirEntry = overdueDirPrefix + TimeUtil.getTodayDir() + "/";
        File dir = new File(overdueDirEntry);
        if (!dir.isDirectory()) // 创建必要的目录
            dir.mkdirs();
        File file = new File(overdueDirEntry + overdueFilePrefix + planId + overdueFileSuffix);
        if (!file.exists()) {
            try {
                file.createNewFile(); // 生成空的过期XML文件
                LOG.info("create overdue file for plan_id: " + planId);
            } catch (IOException e) {
                e.printStackTrace();
                LOG.error("fail to create overdue file for plan_id: " + planId);
            }
        } else {
            LOG.warn("overdue file for plan_id: " + planId + " has already been generated before");
        }
        // XXX: 为实时报表停止Storm任务
        // String shellScript =
        // ClassLoader.getSystemResource("storm-kill.sh").getFile();
        // OuterExeProcess.execute("/bin/bash " + shellScript + " " + planId);
        // LOG.info("kill storm topology: effect_plan_" + planId);
    }

    /**
     * 辅助函数，生成路径配置信息（默认路径配置）
     * 
     * @param xmlGen
     * @param srcPathNode
     * @param srcTypeCommon
     * @param effectUrl
     * @param url2TypeIdMap
     * @param isMidPageInd
     * @param baseType
     *            SrcPathConfig.DEFAULT_PATH_ID_BASE:生成一般来源的路径规则；
     *            SrcPathConfig.OUTER_ADS_PATH_ID_BASE: 生成外投广告的路径规则；
     *            SrcPathConfig.SPM_ADS_PATH_ID_BASE: 生成SPM的路径规则。
     */
    private void generateSrcPath(XMLGenerator xmlGen, Element srcPathNode, String srcTypeCommon, String effectUrl,
            Map<String, Integer> url2TypeIdMap, boolean isMidPageInd, int baseType) {
        if (baseType == SrcPathConfig.DEFAULT_PATH_ID_BASE && (srcTypeCommon == null || srcTypeCommon.equals(""))) { // srcTypeCommon为空时不生成默认路径
            return;
        }
        List<KeyValuePair> attrList = new ArrayList<KeyValuePair>();
        Element ruleNode0 = xmlGen.createElement(srcPathNode, "rule");
        xmlGen.createElement(ruleNode0, "priority", String.valueOf(SrcPathConfig.PRIORITY));
        Element limitNode0 = xmlGen.createElement(ruleNode0, "limit");
        xmlGen.createElement(limitNode0, "num", String.valueOf(SrcPathConfig.LIMIT_NUM));
        xmlGen.createElement(limitNode0, "effect_id", String.valueOf(SrcPathConfig.LIMIT_EFFECT_ID));
        xmlGen.createElement(ruleNode0, "path_id", String.valueOf(baseType));
        Element pathNode0 = xmlGen.createElement(ruleNode0, "path");
        // 为默认路径创建首尾两个node节点
        int nodeId = 0;
        attrList.clear();
        if (baseType == SrcPathConfig.DEFAULT_PATH_ID_BASE) {
            attrList.add(new KeyValuePair("id", String.valueOf(nodeId++)));
            attrList.add(new KeyValuePair("type_refs", srcTypeCommon));
            attrList.add(new KeyValuePair("next", "1"));
            attrList.add(new KeyValuePair("expand", "ptype"));
        } else if (baseType == SrcPathConfig.OUTER_ADS_PATH_ID_BASE) {
            attrList.add(new KeyValuePair("id", String.valueOf(nodeId++)));
            attrList.add(new KeyValuePair("type_refs", "100"));
            attrList.add(new KeyValuePair("next", "1"));
            attrList.add(new KeyValuePair("expand", ":adid"));
        } else if (baseType == SrcPathConfig.SPM_ADS_PATH_ID_BASE) {
            attrList.add(new KeyValuePair("id", String.valueOf(nodeId++)));
            attrList.add(new KeyValuePair("type_refs", "103"));
            attrList.add(new KeyValuePair("next", "1"));
            attrList.add(new KeyValuePair("expand", ":spm"));
        }
        xmlGen.createElement(pathNode0, "node", attrList);
        attrList.clear();
        attrList.add(new KeyValuePair("id", String.valueOf(nodeId)));
        attrList.add(new KeyValuePair("type_refs", String.valueOf(url2TypeIdMap.get(effectUrl))));
        attrList.add(new KeyValuePair("expand", "rule"));
        xmlGen.createElement(pathNode0, "node", attrList);
        if (isMidPageInd)
            xmlGen.createElement(ruleNode0, "effect_owner", String.valueOf(nodeId - 1));
        else
            xmlGen.createElement(ruleNode0, "effect_owner", String.valueOf(nodeId));
    }

    /**
     * 辅助函数，生成路径配置信息（用户自定义路径）
     * 
     * @param xmlGen
     * @param srcPathNode
     * @param srcTypeCommon
     * @param effectUrl
     * @param pathIdList
     * @param pathId2DataMap
     * @param url2TypeIdMap
     * @param isMidPageInd
     * @param baseType
     *            SrcPathConfig.DEFAULT_PATH_ID_BASE:生成一般来源的路径规则；
     *            SrcPathConfig.OUTER_ADS_PATH_ID_BASE: 生成外投广告的路径规则；
     *            SrcPathConfig.SPM_ADS_PATH_ID_BASE: 生成SPM的路径规则。
     */
    private void generateSrcPath(XMLGenerator xmlGen, Element srcPathNode, String srcTypeCommon, String effectUrl,
            List<Integer> pathIdList, Map<Integer, List<PathDataRecord>> pathId2DataMap,
            Map<String, Integer> url2TypeIdMap, boolean isMidPageInd, int baseType) {
        if (baseType == SrcPathConfig.DEFAULT_PATH_ID_BASE && (srcTypeCommon == null || srcTypeCommon.equals(""))) { // srcTypeCommon为空时不生成默认路径
            return;
        }
        List<KeyValuePair> attrList = new ArrayList<KeyValuePair>();
        for (int pathIdIndex = 0; pathIdIndex < pathIdList.size(); pathIdIndex++) {
            Integer pathId = pathIdList.get(pathIdIndex);
            List<PathDataRecord> pathDataRecords = pathId2DataMap.get(pathId);
            // 依次遍历当前path_id对应path中的各个node为其生成路径配置
            Element ruleNode1 = xmlGen.createElement(srcPathNode, "rule");
            xmlGen.createElement(ruleNode1, "priority", String.valueOf(SrcPathConfig.PRIORITY));
            Element limitNode1 = xmlGen.createElement(ruleNode1, "limit");
            xmlGen.createElement(limitNode1, "num", String.valueOf(SrcPathConfig.LIMIT_NUM));
            xmlGen.createElement(limitNode1, "effect_id", String.valueOf(SrcPathConfig.LIMIT_EFFECT_ID));
            xmlGen.createElement(ruleNode1, "path_id", String.valueOf(pathId + baseType));
            Element pathNode1 = xmlGen.createElement(ruleNode1, "path");
            int nodeId = 0;
            attrList.clear();
            if (baseType == SrcPathConfig.DEFAULT_PATH_ID_BASE) {
                attrList.add(new KeyValuePair("id", String.valueOf(nodeId)));
                attrList.add(new KeyValuePair("type_refs", srcTypeCommon));
                attrList.add(new KeyValuePair("next", "1"));
                attrList.add(new KeyValuePair("expand", "ptype"));
            } else if (baseType == SrcPathConfig.OUTER_ADS_PATH_ID_BASE) {
                attrList.add(new KeyValuePair("id", String.valueOf(nodeId)));
                attrList.add(new KeyValuePair("type_refs", "100"));
                attrList.add(new KeyValuePair("next", "1"));
                attrList.add(new KeyValuePair("expand", ":adid"));
            } else if (baseType == SrcPathConfig.SPM_ADS_PATH_ID_BASE) {
                attrList.add(new KeyValuePair("id", String.valueOf(nodeId)));
                attrList.add(new KeyValuePair("type_refs", "103"));
                attrList.add(new KeyValuePair("next", "1"));
                attrList.add(new KeyValuePair("expand", ":spm"));
            }
            xmlGen.createElement(pathNode1, "node", attrList);
            nodeId++;
            for (PathDataRecord dataRecord : pathDataRecords) {
                attrList.clear();
                attrList.add(new KeyValuePair("id", String.valueOf(nodeId)));
                attrList.add(new KeyValuePair("type_refs", String.valueOf(url2TypeIdMap.get(dataRecord.getUrl()))));
                attrList.add(new KeyValuePair("next", String.valueOf(dataRecord.getStep())));
                attrList.add(new KeyValuePair("expand", "rule"));
                xmlGen.createElement(pathNode1, "node", attrList);
                nodeId++;
            }
            attrList.clear();
            attrList.add(new KeyValuePair("id", String.valueOf(nodeId)));
            attrList.add(new KeyValuePair("type_refs", String.valueOf(url2TypeIdMap.get(effectUrl))));
            attrList.add(new KeyValuePair("expand", "rule"));
            xmlGen.createElement(pathNode1, "node", attrList);
            if (isMidPageInd)
                xmlGen.createElement(ruleNode1, "effect_owner", String.valueOf(nodeId - 1));
            else
                xmlGen.createElement(ruleNode1, "effect_owner", String.valueOf(nodeId));
        }
    }

    /**
     * 主函数入口
     * 
     * @param args
     */
    public static void main(String[] args) {
        // TODO Auto-generated method stub
        if (args.length > 0) {
            ConfigLoader.loadConf(args[0]);
        } else {
            String propsFileName = ClassLoader.getSystemResource("config2xml.properties").getFile();
            ConfigLoader.loadConf(propsFileName);
        }

        // XXX 通过标记文件只允许一个ConfigExchanger实例运行
        File flagFile = new File("iambusy.exist");
        if (flagFile.exists()) {
            LOG.error("another process is exchanging xml config from mysql database");
        } else {
            boolean success = false;
            try {
                success = flagFile.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
                success = false;
            }
            if (success) {
                LOG.info("start one process to exchange xml config from mysql database");
                MySQLManager dbMgr = new MySQLManager(new JDBCUtil());
                ConfigExchanger exchanger = new ConfigExchanger(dbMgr);
                exchanger.exchangeConfig();
                flagFile.delete();
            } else {
                LOG.error("fail to create iambusy.exist: file already exists, or an I/O error occurrs");
            }
        }
    }

}
