package com.etao.lz.dw.util;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.HashPartitioner;



public class TextPairKeyPartitioner <K2 extends WritableComparable<Text>, V2 extends Writable>
	extends HashPartitioner<K2, V2> {
	
	public int getPartition(TextPair key, BytesWritable value,
			int numPartitions) {
		return (key.getFirst().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}
}
