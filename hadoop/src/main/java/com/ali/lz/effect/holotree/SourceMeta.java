package com.ali.lz.effect.holotree;

/**
 * 来源路径元数据（归属点时戳和优先级）
 * 
 * @author wxz
 * 
 */
public class SourceMeta {

    // 当前来源路径在全息树上多次出现时，首次出现的归属点时戳
    private long firstOpTS;
    // 当前来源路径在全息树上多次出现时，末次出现的归属点时戳
    private long lastOpTS;
    private int priority;
    // 当前来源路径在全息树上多次出现时，首次出现的效果页节点
    private HoloTreeNode firstEP;
    // 当前来源路径在全息树上多次出现时，末次出现的效果页节点
    private HoloTreeNode lastEP;

    public SourceMeta() {
        firstOpTS = 0;
        lastOpTS = 0;
        priority = Integer.MAX_VALUE;
        firstEP = null;
        lastEP = null;
    }

    public SourceMeta(HoloTreeNode epNode) {
        this();
        firstEP = lastEP = epNode;
        // 其他来源，直接将效果页时戳置为当前效果页节点时戳
        firstOpTS = lastOpTS = (Long) epNode.getPtLogEntry().get("ts");
    }

    public SourceMeta(int priority, HoloTreeNode opNode, HoloTreeNode epNode) {

        this.priority = priority;

        firstOpTS = lastOpTS = (Long) opNode.getPtLogEntry().get("ts");
        firstEP = lastEP = epNode;
    }

    /**
     * 仅限于合并同来源路径
     * 
     * @param meta
     */
    public void mergeSourceMeta(SourceMeta meta) {
        if (meta.firstOpTS < firstOpTS) {
            firstOpTS = meta.firstOpTS;
            firstEP = meta.firstEP;
        }
        if (meta.lastOpTS > lastOpTS) {
            lastOpTS = meta.lastOpTS;
            lastEP = meta.lastEP;
        }
    }

    /**
     * 获取来源路径首次归属点时戳
     * 
     * @return 秒级归属点时戳
     */
    public long getFirstOpTS() {
        return firstOpTS;
    }

    /**
     * 获取来源路径末次归属点时戳
     * 
     * @return 秒级归属点时戳
     */
    public long getLastOpTS() {
        return lastOpTS;
    }

    /**
     * 获取来源路径首次出现时的效果页节点
     * 
     * @return 效果页节点引用
     */
    public HoloTreeNode getFirstEP() {
        return firstEP;
    }

    /**
     * 获取来源路径末次出现时的效果页节点
     * 
     * @return 效果页节点引用
     */
    public HoloTreeNode getLastEP() {
        return lastEP;
    }

    /**
     * 获取来源路径对应规则模板的优先级
     * 
     * @return 规则模板优先级
     */
    public int getPriority() {
        return priority;
    }

}
