package com.etao.lz.dw.util;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

public class KeyAsDirNameOutputFormat {
	public static class KeyAsDirNameSequenceFileOutputFormat<K extends WritableComparable<?>, V extends Writable>
			extends MultipleSequenceFileOutputFormat<K, V> {

		@Override
		protected K generateActualKey(K key, V value) {
			return key;
		}

		@Override
		protected String generateFileNameForKeyValue(K key, V value, String name) {
			return key.toString() + "/" + name;
		}
	}

	public static class KeyAsDirNameTextOutputFormat<K extends WritableComparable<?>, V extends Writable>
			extends MultipleTextOutputFormat<K, V> {

		@Override
		protected K generateActualKey(K key, V value) {
			return null;
		}

		@Override
		protected String generateFileNameForKeyValue(K key, V value, String name) {
			return key.toString() + "/" + name;
		}
	}
}
