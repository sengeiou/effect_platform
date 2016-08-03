package com.etao.lz.effect.rule;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.etao.lz.effect.HoloConfig;
import com.etao.lz.effect.HoloConfig.PathNode;
import com.etao.lz.effect.HoloTreeNode;
import com.etao.lz.effect.PTLogEntry;
import com.etao.lz.effect.SourceMeta;

/**
 * 匹配规则类，负责按照对应的路径规则确定当前全息树节点是否匹配。
 * 
 * @author minzhan
 * 
 */
public class Rule {

	private static final Logger log = LoggerFactory.getLogger(Rule.class);

	private int pathID;
	private int priority;

	private List<RuleNode> ruleNodeList = new ArrayList<RuleNode>();
	// 当前匹配的进度，节点的索引
	private int curIndex = 0;
	// 当前匹配节点，与curIndex对应
	private RuleNode curRuleNode;

	// 目前经过的中间跳数
	private int skip = -1;
	// 是否忽略下一个输入
	private boolean ignoreNext = false;

	// 配置文件中规则配置对象
	private HoloConfig.PathRule pathRuleConf;

	// 效果页(规则最后一跳)类型数组
	private int[] effectPageType = null;

	private int effectPType = 0;
	// 效果页RType(第一个效果页的RType)
	private int effectRType = 0;
	// 规则中效果页到下一个规则节点的跳数
	private int effectSkip = 0;
	// 是否折叠
	private boolean folder = false;
	// 保存规则各级节点对应的 expand 展开值（沿路径配置从后向前排列）
	private List<String> expands = new LinkedList<String>();
	private List<HoloTreeNode> effectNodes = new LinkedList<HoloTreeNode>();
	// 保存当前路径节点下一跳的日志引用，以便 expand 时选择当前节点或下一跳的属性
	private PTLogEntry sonLogEntry = null;

	// 颜色
	private String source = null;
	// 归属点
	private HoloTreeNode opNode = null;

	// 继续expand RType所代表的节点的属性
	private boolean parentExpand = false;
	private boolean over = false;
	private boolean failed = false;
	private int matchedRType = 0;

	// 是否染色
	private boolean colorEnabled = false;

	public Rule(HoloConfig.PathRule pathRuleConf) {
		this.pathRuleConf = pathRuleConf;

		prepare();
	}

	protected void prepare() {
		ArrayList<PathNode> pathNodes = pathRuleConf.node;

		pathID = pathRuleConf.path_id;
		priority = pathRuleConf.priority;

		int length = pathNodes.size();
		int i = length - 1;
		// 归属点索引
		int ownerIndex = pathRuleConf.effect_owner;

		effectPageType = pathNodes.get(i).type_refs;

		// 倒序遍历路径规则, 添加到RuleNode List中
		for (; i >= 0; i--) {
			PathNode child = pathNodes.get(i);

			// 跳数由父亲指向儿子，反转为儿子指向父亲
			int tmpSkip = 0;
			if (i - 1 >= 0) {
				PathNode parent = pathNodes.get(i - 1);
				tmpSkip = parent.next;
			}

			RuleNode ruleNode = new RuleNode(child.type_refs, child.expand,
					tmpSkip);

			// 归属点
			if (i == ownerIndex) {
				ruleNode.setOwnerPoint(true);
			}

			ruleNodeList.add(ruleNode);
		}

		curRuleNode = ruleNodeList.get(0);
	}

	public boolean matchNext(HoloTreeNode treeNode) {
		if (treeNode == null) {
			return false;
		}

		PTLogEntry logEntry = treeNode.getPtLogEntry();
		int pType = logEntry.getPType();
		int curSkip = curRuleNode.getSkip();

		if (sonLogEntry == null) {
			sonLogEntry = logEntry;
		}

		// rType已经匹配上，忽略匹配该节点，只是做属性展开
		if (ignoreNext) {
			ignoreNext = false;

			if (curRuleNode.isOwnerPoint()) {
				opNode = treeNode;
			}

			if (parentExpand) {
				parentExpand = false;
				expand(logEntry, matchedRType, curRuleNode.getExpand());
			}

			if (curSkip == 0) {
				over = true;
				return false;
			}

			sonLogEntry = logEntry;
			return true;
		}

		// this.skip为-1， 代表第一个节点
		if (skip == -1) {
			Set<Integer> typeIDs = curRuleNode.getTypeIDs();

			// 效果页折叠
			if (folder) {
				if (effectPType != 0
						&& (effectPType == pType || effectPType == effectRType)) {
					effectRType = logEntry.getRType();
					effectNodes.add(treeNode);
					if (curSkip == 0) {
						over = true;
						sonLogEntry = logEntry;
						return true;
					} else if (treeNode.getParent() != null) {
						sonLogEntry = logEntry;
						return true;
					} else {
						// 节点为根
						skip = 0;
					}
				} else {
					// 效果页折叠完毕

					// 匹配第一个效果页 rtype
					if (effectSkip == 1) {
						// 规则节点向前移动
						curRuleNode = ruleNodeList.get(curIndex + 1);
						int nextCurSkip = curRuleNode.getSkip();

						Set<Integer> curTypeIDs = curRuleNode.getTypeIDs();

						if (curTypeIDs.contains(effectRType)
								|| typeIDs.contains(-1)) {

							expand(logEntry, effectRType,
									curRuleNode.getExpand());
							if (curRuleNode.isOwnerPoint()) {
								opNode = treeNode;
							}

							if (nextCurSkip == 0) {
								over = true;
								return false;
							}
							++curIndex;
							skip = 0;
							sonLogEntry = logEntry;
							return true;
						}
						skip = 1;
					} else {
						if (effectSkip == 0) {
							over = true;
							return false;
						}
						// 向前一跳
						skip = 1;
					}
				}
			} else {
				// 倒数第一个效果页匹配PType
				if (typeIDs.contains(pType) || typeIDs.contains(-1)) {
					effectPType = pType;
					effectSkip = curSkip;
					effectRType = logEntry.getRType();
					expand(logEntry, pType, curRuleNode.getExpand());
					effectNodes.add(treeNode);
					folder = true;
				} else {
					failed = true;
					return false;
				}

				if (curRuleNode.isOwnerPoint()) {
					opNode = treeNode;
				}

				// 代表最后一个规则节点
				if (curSkip == 0) {
					over = true;
					sonLogEntry = logEntry;
					return true;
				} else if (treeNode.getParent() != null) {
					sonLogEntry = logEntry;
					return true;
				} else {
					// 节点为根
					skip = 0;
				}
			}
		}

		// 一跳未至规则节点
		if (skip + 1 < curSkip) {
			// 向前一跳
			skip++;
			sonLogEntry = logEntry;
			return true;
		}

		// 到达当前节点
		if (skip == curSkip) {
			// 规则节点向前移动
			curRuleNode = ruleNodeList.get(++curIndex);
			curSkip = curRuleNode.getSkip();

			Set<Integer> typeIDs = curRuleNode.getTypeIDs();
			// 匹配PType
			if (pType != 0 && (typeIDs.contains(pType) || typeIDs.contains(-1))) {
				expand(logEntry, pType, curRuleNode.getExpand());
			} else {
				failed = true;
				return false;
			}

			if (curRuleNode.isOwnerPoint()) {
				opNode = treeNode;
			}

			// 代表最后一个节点
			if (curSkip == 0) {
				over = true;
				return false;
			}

			// 从新节点重新开始计数
			skip = 0;

		}

		// 一跳至规则节点, 此时匹配rtype与下一规则节点
		if (skip + 1 == curSkip) {
			// 临时规则节点向前移动
			RuleNode nextRuleNode = ruleNodeList.get(curIndex + 1);
			int nextSkip = nextRuleNode.getSkip();

			Set<Integer> typeIDs = nextRuleNode.getTypeIDs();
			int rType = logEntry.getRType();
			if (typeIDs.contains(rType) || typeIDs.contains(-1)) {
				// 匹配上rtype，忽略下个父节点输入
				ignoreNext = true;

				// 匹配成功，更新当前规则节点以及相关偏移
				curRuleNode = nextRuleNode;
				curIndex++;

				HoloTreeNode parent = treeNode.getParent();
				if (parent == null) {
					// 已经到达根节点
					
					sonLogEntry = logEntry;
					expand(null, rType, curRuleNode.getExpand());

					// 为了兼容自己就是根的情形
					// 下个规则节点为归属点，但是与规则点对应的父节点为null，则将归属点置为此根节点
					if (curRuleNode.isOwnerPoint()) {
						opNode = treeNode;
					}
				} else {
					parentExpand = true;
					matchedRType = rType;
				}

				if (nextSkip == 0) {
					sonLogEntry = logEntry;
					over = true;
					return (parent == null) ? false : true;
				}
			}
		}

		skip++;
		sonLogEntry = logEntry;
		return true;
	}

	private void expand(PTLogEntry curLogEntry, int typeID, String expand) {
		String expRes = null;
		if ("rule".equalsIgnoreCase(expand)) {
			// 当前节点要按 rule 级别展开，结果为 path id
			expRes = String.valueOf(pathID);
		} else if ("ptype".equalsIgnoreCase(expand)) {
			// 当前节点要按 ptype 级别展开，结果为 type id
			expRes = String.valueOf(typeID);
		} else if (expand.startsWith(":")) {
			// 当前节点要按 :<prop> 属性级别展开，结果为对应日志的属性值
			String prop = expand.substring(1);
			// 需要根据 <prop> 属性的匹配目标决定使用当前节点下一跳中的值还是当前节点中的值
			if (sonLogEntry.getSourceType(prop) == HoloConfig.MATCH_REFERER) {
				// 下一跳中的属性值指明其目标为当前节点
				expRes = String.valueOf(sonLogEntry.get(prop));
			} else {
				if (curLogEntry != null) {
					expRes = String.valueOf(curLogEntry.get(prop));
				} else {
					// 属性需要取父节点的，但是自己已经是根节点的情形
					expRes = "";
				}
			}
		} else {
			log.warn("unknown expand value, default to 'rule' level: " + expand);
			expRes = String.valueOf(pathID);
		}

		expands.add(expRes);
	}

	private void generateSource() {
		StringBuilder sb = new StringBuilder();
		sb.append(pathID);
		sb.append("\002");

		Collections.reverse(expands);

		// 拼装完成来源路径展开标识串
		boolean first = true;
		for (String exp : expands) {
			if (first) {
				first = false;
			} else {
				sb.append("\003");
			}
			sb.append(exp);
		}

		source = sb.toString();
	}

	private void colorEffectNodes() {
		for (HoloTreeNode effectNode : effectNodes) {
			colorEffectNode(effectNode);
		}
	}

	private void colorEffectNode(HoloTreeNode effectNode) {
		SourceMeta meta = new SourceMeta(priority, opNode, effectNodes.get(0));
		effectNode.addSource(source, meta);
	}

	/**
	 * 重置匹配的中间状态
	 */
	public void reset() {

		if (matched() && colorEnabled) {
			generateSource();
			// 标记为效果页
			for (HoloTreeNode treeNode : effectNodes) {
				treeNode.setEffectPage(true);
			}
			colorEffectNodes();
		}

		colorEnabled = false;
		over = false;
		failed = false;

		curIndex = 0;
		curRuleNode = ruleNodeList.get(0);
		skip = -1;
		ignoreNext = false;

		expands.clear();
		sonLogEntry = null;
		parentExpand = false;
		matchedRType = 0;

		effectNodes.clear();
		opNode = null;
		effectPType = 0;
		effectRType = 0;
		effectSkip = 0;
		folder = false;
		source = null;
	}

	public void setColorEnabled(boolean colorEnabled) {
		this.colorEnabled = colorEnabled;
	}

	public boolean matched() {
		return over && !failed;
	}

	/**
	 * @return 规则优先级
	 */
	public int getPriority() {
		return pathRuleConf.priority;
	}

	/**
	 * 返回效果页的类型数组
	 * 
	 * @return 效果页类型数组，-1代表'*'（任意类型）
	 */
	public int[] getEffectPageType() {
		return effectPageType;
	}
}
