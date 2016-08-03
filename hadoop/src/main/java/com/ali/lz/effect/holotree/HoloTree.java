package com.ali.lz.effect.holotree;

import java.util.SortedMap;

/**
 * 全息树访问接口
 * 
 * 独立为接口而不是直接使用 SortedMap 是为了方便增加树级别的信息。
 * 
 * @author wxz
 * 
 */
public interface HoloTree extends SortedMap<Long, HoloTreeNode> {

}
