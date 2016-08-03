package com.etao.lz.effect.rule;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import com.etao.lz.effect.HoloConfig;
import com.etao.lz.effect.HoloConfig.PathRule;
import com.etao.lz.effect.HoloTreeNode;

/**
 * 匹配规则集合类，驱动各条规则的全息树匹配操作，并根据规则优先级调整匹配结果。
 * 
 * @author minzhan
 * 
 */
public class RuleSet {

	private HoloConfig conf;
	private ArrayList<Rule> ruleList = new ArrayList<Rule>();
	private Set<Integer> effectPageSet = new HashSet<Integer>();

	// 存储当前状态，尚可能匹配的规则
	private LinkedList<Rule> ruleListToMatch = new LinkedList<Rule>();
	// 匹配成功的规则
	private LinkedList<Rule> resultList = new LinkedList<Rule>();

	// 匹配的规则当前优先级
	private int curPriority = -1;

	/**
	 * 多个规则的集合类
	 * 
	 * @param conf
	 *            配置文件对象，用户的规则在此
	 */
	public RuleSet(HoloConfig conf) {
		this.conf = conf;
		generateRules();
	}

	/**
	 * 依据配置文件，生成规则对象集合
	 */
	private void generateRules() {
		int n = conf.getPathRuleCount();

		for (int i = 0; i < n; i++) {
			PathRule r = conf.getPathRule(i);

			Rule rule = new Rule(r);

			int[] effectPageType = rule.getEffectPageType();
			for (int t : effectPageType) {
				effectPageSet.add(t);
			}
			ruleList.add(rule);
			ruleListToMatch.add(rule);
		}

	}

	/**
	 * 返回效果页类型集合Set
	 * 
	 * @return 效果页类型集合Set
	 */
	public Set<Integer> getEffectPageSet() {
		return effectPageSet;
	}

	/**
	 * 匹配TreeNode, 如果匹配完成或者所有均无法匹配成功，返回false，否则true
	 * 
	 * @param treeNode
	 * @return
	 */
	public boolean matchNext(HoloTreeNode treeNode) {

		LinkedList<Rule> tmpRuleList = new LinkedList<Rule>();

		for (Rule r : ruleListToMatch) {

			int priority = r.getPriority();
			// 由于从 HoloConfig 获得的规则列表是按优先级从高到低排序的，因此若当前规则的优先级低于已匹配成功
			// 规则的优先级，就意味着后续规则优先级全都低于该优先级，不需匹配可直接返回
			if (curPriority != -1 && priority > curPriority) {
				return false;
			}

			if (r.matchNext(treeNode)) {
				tmpRuleList.add(r);
			} else {

				// 匹配成功
				if (r.matched()) {
					// 如果高优先级的匹配，清除低优先级的结果
					if (priority < curPriority) {
						resultList.clear();
					}
					resultList.add(r);
					curPriority = priority;
				}
			}
		}

		ruleListToMatch = tmpRuleList;

		// 仍然有需要匹配的规则
		if (ruleListToMatch.size() > 0) {
			return true;
		}

		return false;
	}

	/**
	 * 重置所有状态，等待下一轮匹配
	 */
	public void reset() {

		// 设置匹配成功规则的待染色标志
		for (Rule r : resultList) {
			r.setColorEnabled(true);
		}

		ruleListToMatch.clear();
		for (Rule r : ruleList) {
			// 各条规则重置状态，同时根据待染色标志进行必要的染色操作
			r.reset();
			ruleListToMatch.add(r);
		}

		curPriority = -1;
		resultList.clear();

	}
}
