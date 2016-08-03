package com.ali.lz.effect.extendutils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import com.ali.lz.effect.hadooputils.TextPair;

public class ReduceDriverUtil<K1, V1, K2, V2> extends ReduceDriver<TextPair, BytesWritable, Text, Text> {

    /**
     * 测试数据控制台
     * 
     * @param key
     * @param values
     * @param outputKey
     * @param expectedOutputFile
     */
    public void runSingleKeyConfTest(TextPair key, List<BytesWritable> values, String outputKey,
            String expectedOutputFile) {

        try {
            this.withInput(key, values);

            List<Pair<Text, Text>> reduceOutputs = this.run();

            if (expectedOutputFile != null) {
                setMultiOutputsfromFile(expectedOutputFile, reduceOutputs, null, outputKey);
            } else {
                for (Pair<Text, Text> out : reduceOutputs) {
                    System.out.println(out.getSecond());
                }
            }
        } catch (final IOException ioe) {
            LOG.error(ioe);
            throw new RuntimeException(ioe);
        }
    }

    private void setMultiOutputsfromFile(String expectedOutputFile, List<Pair<Text, Text>> reduceOutputs,
            String keyValueSplitter, String validateKey) throws FileNotFoundException, IOException {
        // expectedOutputFile =
        // ClassLoader.getSystemResource(expectedOutputFile)
        // .getFile();
        BufferedReader r_out = new BufferedReader(new InputStreamReader(new FileInputStream(expectedOutputFile)));
        String line;
        while ((line = r_out.readLine()) != null) {
            if (keyValueSplitter == null || keyValueSplitter.isEmpty())
                this.expectedOutputs.add(new Pair<Text, Text>(new Text(validateKey), new Text(line)));
            else {
                String[] kv = line.split(keyValueSplitter, 2);
                if (kv.length == 2) {
                    this.expectedOutputs.add(new Pair<Text, Text>(new Text(kv[0]), new Text(kv[1])));
                }
            }
        }

        validate(reduceOutputs, true);
        validate(counterWrapper);
    }
}
