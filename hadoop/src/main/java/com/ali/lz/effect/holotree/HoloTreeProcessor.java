package com.ali.lz.effect.holotree;

public interface HoloTreeProcessor {

    /**
     * 当前节点插入树后被染色后，触发该事件
     * 
     * @param conf
     *            全息树配置
     * @param curNode
     *            染色的当前节点
     */
    void onColoredNode(HoloConfig conf, HoloTreeNode curNode);

    /**
     * 插入当前节点后，树达到session最大个数时，或者新开一个session时，全息树会触发结束事件
     * 
     * @param conf
     *            全息树配置
     * @param curNode
     *            结束的全息树的最后一个节点
     */
    void onCompleteTree(HoloConfig conf, HoloTreeNode lastNode);

    /**
     * 当前节点被判定为效果页面后，触发该事件
     * 
     * @param conf
     *            全息树配置
     * @param effectNode
     *            当前效果页节点
     */
    void onEffectPage(HoloConfig conf, HoloTreeNode effectNode);

    /**
     * 当效果页的子节点被插入并染色后，触发该事件
     * 
     * @param conf
     *            全息树配置
     * @param effectNode
     *            效果页的子节点（当前插入的几点）
     */
    void onEffectPageChild(HoloConfig conf, HoloTreeNode childNode);

    /**
     * 新树被创建时，触发该事件
     * 
     * @param conf
     *            全息树配置
     * @param rootNode
     *            全息树的根节点
     */
    void onNewTree(HoloConfig conf, HoloTreeNode rootNode);
}
