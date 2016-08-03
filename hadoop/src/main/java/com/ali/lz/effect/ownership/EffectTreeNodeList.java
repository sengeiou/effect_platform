package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import com.ali.lz.effect.hadooputils.EffectJobStatusCounter;
import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.holotree.HoloSet;
import com.ali.lz.effect.holotree.HoloTreeBuilder;
import com.ali.lz.effect.holotree.HoloTreeNode;
import com.ali.lz.effect.holotree.PTLogEntry;
import com.ali.lz.effect.proto.LzEffectProto.TreeNodeValue;
import com.ali.lz.effect.proto.LzEffectProtoUtil;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;

public class EffectTreeNodeList {
    private OutputCollector<Text, Text> output = null;
    private Reporter reporter = null;

    private List<TreeNodeValue> treeNodes = new ArrayList<TreeNodeValue>();

    private HoloSet holoKits = null;

    // 节点List存储阀值,超出阀值时会触发建树以及清空操作
    private int capacity = 2000;

    public EffectTreeNodeList() {

    }

    public EffectTreeNodeList(HoloSet holoKits, int capacity) {
        this.holoKits = holoKits;
        this.capacity = capacity;
    }

    public int getCapacity() {
        return capacity;
    }

    public void setCapacity(int capacity) {
        this.capacity = capacity;
    }

    /**
     * 传hadoop的输出相关类
     * 
     * @param output
     * @param reporter
     */
    public void setOutputParam(OutputCollector<Text, Text> output, Reporter reporter) {
        this.output = output;
        this.reporter = reporter;
    }

    /**
     * 清空数据
     */
    public void clear() {
        treeNodes.clear();
        holoKits.clearPlans();
    }

    /**
     * 添加树节点到List中，添加过程中判断哪些plan需要保留后续进行建树操作
     * 
     * @param node
     * @throws IOException
     */
    public void append(TreeNodeValue node) throws IOException {
        treeNodes.add(node);
        holoKits.addProcessPlans(node.getTypeRefList());

        // 超出阀值时触发建树以及清空操作
        if (treeNodes.size() >= capacity) {
            buildAllTrees();
            clear();
        }
    }

    /**
     * 创建树, 并输出到output
     * 
     * @param output
     * @throws IOException
     */
    public void buildAllTrees() throws IOException {
        Iterator<Integer> it = holoKits.getProcessPlans().iterator();
        while (it.hasNext()) {
            buildTree(it.next());
        }
    }

    /**
     * 创建指定planId的树, 并输出到output
     * 
     * @param planId
     * @param output
     * @throws IOException
     */
    private void buildTree(Integer planId) throws IOException {
        for (TreeNodeValue node : treeNodes) {
            PTLogEntry logEntry = genLogEntry(node, planId);
            holoKits.getHoloKitMap().get(planId).builder.appendLog(logEntry);
        }

        outputHoloTree(planId);

        // 清空建树器，开始另一轮建树
        holoKits.getHoloKitMap().get(planId).builder.flush();
    }

    /**
     * 建立树的节点信息
     * 
     * @param node
     * @param planId
     */
    private PTLogEntry genLogEntry(TreeNodeValue node, Integer planId) {
        PTLogEntry logEntry = new PTLogEntry();
        List<String> token = new ArrayList<String>();

        // 填充原始日志相关字段信息
        logEntry.put("ts", node.getTs());
        logEntry.put("url", node.getUrl());
        logEntry.put("refer_url", node.getRefer());
        // 保存原始url和refer用于输出 (建树过程中对url, refer_url做过encode和decode, 导致用建树处理完的url,
        // refer_url会出现不可见字符等问题)
        logEntry.put("original_url", node.getUrl());
        logEntry.put("original_refer_url", node.getRefer());
        logEntry.put("shop_id", node.getShopId());
        logEntry.put("auction_id", node.getAuctionId());
        logEntry.put("uid", node.getUserId());
        logEntry.put("ali_corp", node.getAliCorp());
        logEntry.put("mid", node.getCookie());
        // 建树时不需按session截断, 将原始sid存入session中
        logEntry.put("sid", "");
        logEntry.put("session", node.getSession());
        logEntry.put("mid_uid", node.getVisitId());

        List<TreeNodeValue.KeyValueS> access_useful_extras = node.getAccessUsefulExtraList();
        for (TreeNodeValue.KeyValueS userful_extra : access_useful_extras) {
            logEntry.put(userful_extra.getKey(), userful_extra.getValue());
            token.add(userful_extra.getKey() + Constants.CTRL_C + userful_extra.getValue());
        }

        logEntry.put("access_extra", node.getAccessExtra());

        // 找到需要的plan_id生成logEntry
        for (TreeNodeValue.TypeRef typeRef : node.getTypeRefList()) {
            if (typeRef.getPlanId() == planId) {
                logEntry.put("analyzer_id", typeRef.getAnalyzerId());
                logEntry.put("plan_id", typeRef.getPlanId());
                logEntry.setRType(typeRef.getRtype());
                logEntry.setPType(typeRef.getPtype());
                logEntry.setMatched(typeRef.getIsMatched());
                for (TreeNodeValue.KeyValueS captureInfo : typeRef.getCapturedInfoList()) {
                    logEntry.put(captureInfo.getKey(), captureInfo.getValue());
                    token.add(captureInfo.getKey() + Constants.CTRL_C + captureInfo.getValue());
                }
                for (TreeNodeValue.KeyValueI sourceInfo : typeRef.getSourceInfoList()) {
                    logEntry.getSourceType().put(sourceInfo.getKey(), sourceInfo.getValue());
                }
            }
        }

        logEntry.put("access_useful_extras", StringUtil.join(token, Constants.CTRL_B));
        logEntry.put("page_duration", 0L);
        return logEntry;
    }

    /**
     * 生成输出树结点的对象列表
     * 
     * @param treeBuilder
     * @throws IOException
     */
    public void outputHoloTree(Integer planId) throws IOException {

        HoloTreeBuilder treeBuilder = holoKits.getHoloKitMap().get(planId).builder;
        HoloConfig holoConfig = treeBuilder.getHoloConfig();

        // 循环遍历树并输出每棵树的结点
        for (SortedMap<Long, HoloTreeNode> holoTree : treeBuilder.getCurrentTrees()) {
            Iterator<HoloTreeNode> it = holoTree.values().iterator();
            while (it.hasNext()) {
                // 轮询树中的每个树结点
                HoloTreeNode node = it.next();

                TreeNodeValue.Builder outputNode = null;
                TreeNodeValue.TypeRef.Builder typeRefBuilder = null;

                // 判断是否只输出染色结点
                if (!node.getSources().isEmpty()) {
                    outputNode = EffectHoloNodeToProto.genBuilder(node, holoConfig.doHjljOwnership);
                    typeRefBuilder = EffectHoloNodeToProto.genColorTypeRefBuilder(node, holoConfig.lookahead);
                } else if (holoConfig.is_all) {
                    outputNode = EffectHoloNodeToProto.genBuilder(node, holoConfig.doHjljOwnership);
                    if (holoConfig.root_is_lp) {
                        typeRefBuilder = EffectHoloNodeToProto.genRootIsLPTypeRefBuilder(node, holoConfig.lookahead);
                        EffectHoloNodeToProto.inheritEPSrcProperties(node, outputNode, holoConfig.doHjljOwnership);
                    } else {
                        typeRefBuilder = EffectHoloNodeToProto.genNormalTypeRefBuilder(node);
                    }
                }

                if (outputNode != null) {
                    outputNode.addTypeRef(typeRefBuilder);
                    String data = LzEffectProtoUtil.toString(outputNode.build());
                    output.collect(new Text(planId.toString()), new Text(data));
                    reporter.incrCounter(EffectJobStatusCounter.FinderJobProcessStatus.HOLOTREE_OUTPUT_NODE_COUNT, 1);

                } else {
                    reporter.incrCounter(EffectJobStatusCounter.FinderJobProcessStatus.HOLOTREE_OUTPUT_ERROR, 1);
                }
            }
        }
    }

}
