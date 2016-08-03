package com.ali.lz.effect.hadooputils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.lib.MultipleSequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleTextOutputFormat;

import com.ali.lz.effect.utils.Constants;

/**
 * 构建访问树和效果计算任务的OutputFormat，使得任务输出的数据可以以reduce output的key为目录组织
 * 
 * @author feiqiong.dpf
 * 
 */
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

    public static class KeyAsDirNameTextOutputFormat<K extends WritableComparable<?>, V extends Writable> extends
            MultipleTextOutputFormat<K, V> {

        @Override
        protected K generateActualKey(K key, V value) {
            return null;
        }

        @Override
        protected String generateFileNameForKeyValue(K key, V value, String name) {
            return key.toString() + "/" + name;
        }
    }

    /**
     * 将不同plan的数据输出到同一目录(planId=0目录)下。
     * 如果任务需要读取大量xml计算，且不需并行读取多个(组)xml运行，建议使用该OutputFormat。
     * 避免因同一reduce数据输出到多个plan目录，导致任务产出大量小文件引发文件数配额超标、下游任务list输入文件OOM等诸多问题。
     * 目前应用于天猫品牌站效果分析项目
     */

    public static class MergePlanDirSeqFileOutputFormat<K extends WritableComparable<?>, V extends Writable> extends
            MultipleSequenceFileOutputFormat<K, V> {

        @Override
        protected K generateActualKey(K key, V value) {
            return key;
        }

        @Override
        protected String generateFileNameForKeyValue(K key, V value, String name) {
            return Constants.MERGE_PLANID + "/" + name;
        }
    }

    public static class MergePlanDirTextOutputFormat<K extends WritableComparable<?>, V extends Writable> extends
            MultipleTextOutputFormat<K, V> {

        @Override
        protected K generateActualKey(K key, V value) {
            return null;
        }

        @Override
        protected String generateFileNameForKeyValue(K key, V value, String name) {
            return Constants.MERGE_PLANID + "/" + name;
        }
    }
}
