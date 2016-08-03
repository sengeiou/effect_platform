package com.ali.lz.effect.holotree;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * 全息树节点类
 * 
 */
public class HoloTreeNode {

    private PTLogEntry ptLogEntry;
    private HoloTreeNode parent;

    private List<HoloTreeNode> children;
    private Map<String, SourceMeta> sources;
    private String serialRootPath;
    private String ptypeRootPath;
    private boolean isEffectPage;
    private HoloTree tree;
    // 将日志中的秒级时戳同 session 内节点序号整合，作为单棵树内节点的唯一标示，此外作为HoloTree中树节点排序的key
    private Long uniqTS;
    private long flags;

    public HoloTreeNode(PTLogEntry ptLogEntry) {
        this.ptLogEntry = ptLogEntry;
        this.children = new LinkedList<HoloTreeNode>();
        this.sources = new HashMap<String, SourceMeta>();
    }

    /**
     * @return 返回获取节点的序号路径,为点分隔的各级节点序号, 序号为节点在当前全息树内的顺序编号
     */
    public String getSerialRootPath() {
        return serialRootPath;
    }

    public void setSerialRootPath(String serialRootPath) {
        this.serialRootPath = serialRootPath;
    }

    /**
     * 获取节点的页面类型路径, 转化为 unicode的各级节点页面类型编号,用于来源路径识别
     * 
     * @return 序号路径
     */
    public String getPTypeRootPath() {
        return ptypeRootPath;
    }

    public void setPTypeRootPath(String ptypeRootPath) {
        this.ptypeRootPath = ptypeRootPath;
    }

    /**
     * 获取节点属性集合
     * 
     * @return 节点属性集合
     */
    public PTLogEntry getPtLogEntry() {
        return ptLogEntry;
    }

    /**
     * @return 当前节点已经匹配的来源列表，key为来源路径标识串，value为元数据
     */
    public Map<String, SourceMeta> getSources() {
        return sources;
    }

    /**
     * 增加匹配的来源路径标识
     * 
     * @param source
     *            来源标识字符串
     * @param meta
     *            来源路径的元数据
     */
    public void addSource(String source, SourceMeta meta) {
        // 同来源路径多次出现时不能直接覆盖，需要合并归属点时戳
        if (sources.containsKey(source)) {
            meta.mergeSourceMeta(sources.get(source));
        }
        sources.put(source, meta);
    }

    /**
     * 继承父节点的来源路径标识
     */
    public void inheritSources() {
        if (parent != null) {
            sources.putAll(parent.sources);
        }
    }

    /**
     * @return 父节点
     */
    public HoloTreeNode getParent() {
        return parent;
    }

    /**
     * @param parent
     *            父节点
     */
    public void setParent(HoloTreeNode parent) {
        this.parent = parent;
    }

    /**
     * @return 子节点列表
     */
    public List<HoloTreeNode> getChildren() {
        return children;
    }

    /**
     * 增加子节点
     * 
     * @param child
     *            子节点
     */
    public void appendChild(HoloTreeNode child) {
        children.add(child);
    }

    /**
     * 本节点是否是效果页？
     * 
     * @return
     */
    public boolean isEffectPage() {
        return isEffectPage;
    }

    public void setEffectPage(boolean isEffectPage) {
        this.isEffectPage = isEffectPage;
    }

    /**
     * 获取节点所属的全息树
     * 
     * @return
     */
    public HoloTree getTree() {
        return tree;
    }

    public void setTree(HoloTree tree) {
        this.tree = tree;
    }

    public Long getUniqTS() {
        return uniqTS;
    }

    public void setUniqTS(Long uniqTS) {
        this.uniqTS = uniqTS;
    }

    /**
     * 一个标志数值，统计时可能要用到。
     */
    public void setFlags(long flags) {
        this.flags = flags;
    }

    public long getFlags() {
        return flags;
    }
}
