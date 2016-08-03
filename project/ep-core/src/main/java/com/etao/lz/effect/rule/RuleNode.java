package com.etao.lz.effect.rule;

import java.util.HashSet;
import java.util.Set;

/**
 * RuelNode类是用来表示根据路径规则中的一个节点， 一个RuleNode对象经过一跳或多跳到达父亲RuleNode对象
 */
public class RuleNode {

	private Set<Integer> typeIDs = new HashSet<Integer>();

	private String expand;
	
	// 是否归属点
	private boolean ownerPoint = false;

	// 到父节点的跳数
	private int skip;

	/**
	 * @param typeIDs
	 *            typid数组，路径上某个节点上能够匹配的类型列表
	 * @param expand
	 *            节点的扩展方式， 有rule, ptype, :<keyword>三种方式
	 */
	public RuleNode(int[] typeIDs, String expand, int skip) {
		
		for(int typeID : typeIDs) {
			this.typeIDs.add(typeID);
		}
		this.expand = expand;
		this.skip = skip;
	}

	/**
	 * 获取该路径规则中，节点的扩展方式
	 * 
	 * @return 节点的扩展方式， 有rule, ptype, :<keyword>三种方式
	 */
	public String getExpand() {
		return this.expand;
	}

	/**
	 * 返回RuleNode节点匹配的typeid列表
	 * 
	 * @return typeid数组，tyepid为-1，[0，Integer.MAX_VALUE - OFFSET], 其中-1代表任意'*'
	 */
	public Set<Integer> getTypeIDs() {
		return typeIDs;
	}

	/**
	 * @return 返回到父节点的跳数
	 */
	public int getSkip() {
		return skip;
	}

	public boolean isOwnerPoint() {
		return ownerPoint;
	}

	public void setOwnerPoint(boolean ownerPoint) {
		this.ownerPoint = ownerPoint;
	}
}
