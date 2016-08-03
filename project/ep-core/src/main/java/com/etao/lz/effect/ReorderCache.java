package com.etao.lz.effect;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

public class ReorderCache<T extends LogEntry> {

	private static class ComparableLogEntry<T extends LogEntry> implements
			Comparable<ComparableLogEntry<T>> {

		private T logEntry;

		public ComparableLogEntry(T accessEvent) {
			this.logEntry = accessEvent;
		}

		/*
		 * 按照LogEntry对象的时间戳字段比较
		 * 
		 * @see java.lang.Comparable#compareTo(java.lang.Object)
		 */
		@Override
		public int compareTo(ComparableLogEntry<T> o) {
			return (int) (this.logEntry.getTimeStamp() - o.logEntry
					.getTimeStamp());
		}
	}

	private final List<T> emptyList = new ArrayList<T>();

	private PriorityQueue<ComparableLogEntry<T>> priorityQ;

	/**
	 * Cache中最多存多少条数据
	 */
	private int threshold;

	/**
	 * 每次到达阈值后，一次吐出的数据的条数
	 */
	private int batchNum;

	/**
	 * 初始化{@link #threshold}, {@link batchNum}以及 {{@link #priorityQ}
	 * 
	 * @param threshold
	 *            Cache中最多存多少条数据
	 * @param batchNum
	 *            每次到达阈值后，一次吐出的数据的条数
	 */
	public ReorderCache(int threshold, int batchNum) {
		this.priorityQ = new PriorityQueue<ComparableLogEntry<T>>();
		this.threshold = (threshold >= 0) ? threshold : 0;
		this.batchNum = (batchNum > threshold) ? threshold : batchNum;
		if (this.batchNum < 1)
			this.batchNum = 1;
	}

	/**
	 * 缓存日志对象，同时按照接口方法{@linkplain LogEntry#getTimeStamp() LogEntry时间戳字段}排序， 到达
	 * {@link #threshold}后，会按照时间顺序扇出{@link #batchNum}数量的日志对象
	 * 
	 * @param e
	 *            日志对象
	 * @return 日志对象的List， 如果缓存中未到达{@link #threshold}， 返回空列表
	 */
	public List<T> cache(T e) {

		fanIn(e);

		if (this.priorityQ.size() >= this.threshold) {
			return fanOut();
		} else {
			return emptyList;
		}
	}

	/**
	 * 将缓存的数据全部扇出
	 * 
	 * @return 日志对象的ArrayList
	 */
	public List<T> flush() {
		List<T> entries = new LinkedList<T>();

		while (!this.priorityQ.isEmpty()) {
			entries.add(this.priorityQ.remove().logEntry);
		}
		return entries;
	}

	/**
	 * 清除缓存中所有数据
	 */
	public void clear() {
		this.priorityQ.clear();
	}

	/**
	 * 获取缓存中的日志对象个数
	 * 
	 * @return 缓存中日志对象个数
	 */
	public int getCachedCount() {
		return this.priorityQ.size();
	}

	/**
	 * 查看cache中时间最远的日志对象
	 * 
	 * @return 日志对象, 若cache中无对象，返回null
	 */
	public T peek() {
		ComparableLogEntry<T> c = this.priorityQ.peek();
		if (c == null)
			return null;
		return c.logEntry;
	}

	/**
	 * 取出cache中时间最远的日志对象
	 * 
	 * @return 日志对象， 若cache中无对象，返回null
	 */

	public T pop() {

		if (!this.priorityQ.isEmpty())
			return this.priorityQ.remove().logEntry;
		else
			return null;
	}

	private void fanIn(T e) {
		this.priorityQ.add(new ComparableLogEntry<T>(e));
	}

	private List<T> fanOut() {
		List<T> entries = new LinkedList<T>();

		for (int i = 0; i < batchNum; i++) {
			if (this.priorityQ.isEmpty())
				break;
			else {
				entries.add(this.priorityQ.remove().logEntry);
			}
		}
		return entries;
	}
}
