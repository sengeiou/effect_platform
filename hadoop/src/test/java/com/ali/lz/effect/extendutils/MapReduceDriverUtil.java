package com.ali.lz.effect.extendutils;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;

import com.ali.lz.effect.utils.Constants;

public class MapReduceDriverUtil<K1, V1, K2, V2, K3, V3> extends MapReduceDriver<Object, Text, K2, V2, Text, Text> {

    /**
     * 测试数据控制台
     * 
     * @param input_file_name
     * @param output_key
     *            MR输出数据中的key
     * @param output_file_name
     * @param conf_prop
     */
    public void runSingleKeyConfTest(String input_file_name, String output_key, String output_file_name,
            Map<String, String> conf_prop) {

        this.setConfiguration(MRUnitTools.getConfigurationfromMap(conf_prop));
        try {
            setMultiInputsfromFile(input_file_name);
            List<Pair<Text, Text>> reduceOutputs = this.run();

            if (output_file_name != null) {
                setMultiOutputsfromFile(output_file_name, reduceOutputs, null, output_key);
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

    /**
     * 
     * @param input_file_name
     * @param output_file_name
     *            存放预期结果集的文件，其中每条结果的key，value以':'分割
     * @param conf_prop
     */
    public void runMultiKeysConfTest(String input_file_name, String output_file_name, Map<String, String> conf_prop) {

        this.setConfiguration(MRUnitTools.getConfigurationfromMap(conf_prop));
        try {
            setMultiInputsfromFile(input_file_name);
            List<Pair<Text, Text>> reduceOutputs = this.run();

            if (output_file_name != null) {
                setMultiOutputsfromFile(output_file_name, reduceOutputs, ":", "");
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

    private void setMultiOutputsfromFile(String output_file_name, List<Pair<Text, Text>> reduceOutputs,
            String keyValueSplitter, String validateKey) throws FileNotFoundException, IOException {
        String line;
        // output_file_name = ClassLoader.getSystemResource(output_file_name)
        // .getFile();
        BufferedReader r_out = new BufferedReader(new InputStreamReader(new FileInputStream(output_file_name)));
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

    private void setMultiInputsfromFile(String input_file_name) throws FileNotFoundException, IOException {
        String line;
        // input_file_name = ClassLoader.getSystemResource(input_file_name)
        // .getFile();
        BufferedReader r_in = new BufferedReader(new InputStreamReader(new FileInputStream(input_file_name)));
        while ((line = r_in.readLine()) != null) {
            this.withInput(new Text(""), new Text(line));
        }
    }
}
