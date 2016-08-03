package com.ali.lz.effect.ownership.etao;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ali.lz.effect.hadooputils.KeyAsDirNameOutputFormat;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.hadooputils.TextPairKeyPartitioner;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;

public class EffectETaoTreeRunner extends Configured implements Tool {

    private int numOfMappers = 1;
    private int numOfReducers = 100;
    private boolean isTextFile = false;

    public EffectETaoTreeRunner() {

    }

    public EffectETaoTreeRunner(boolean isTextFile) {
        this.isTextFile = isTextFile;
    }

    private void initJob(JobConf job) {
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputValueGroupingComparator(TextPair.RealComparator.class);
        job.setPartitionerClass(TextPairKeyPartitioner.class);

        job.setNumMapTasks(numOfMappers);
        job.setNumReduceTasks(numOfReducers);

        if (isTextFile) {
            job.setInputFormat(TextInputFormat.class);
        } else {
            job.setInputFormat(SequenceFileInputFormat.class);
        }
        if (isTextFile) {
            job.setOutputFormat(KeyAsDirNameOutputFormat.KeyAsDirNameTextOutputFormat.class);
        } else {
            job.setOutputFormat(KeyAsDirNameOutputFormat.KeyAsDirNameSequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        }
    }

    private int runEtaoTreeJob(String input, String output, Map<String, String> properties) throws IOException {
        JobConf job = new JobConf(getConf(), EffectETaoTreeRunner.class);
        job.setJobName("Lz_Effect_Platform_EtaoTreeBuilder");
        initJob(job);

        for (String propertyKey : properties.keySet()) {
            String value = properties.get(propertyKey);
            job.set(propertyKey, value);
        }
        job.setMapperClass(EffectETaoTreeMapper.class);
        job.setReducerClass(EffectETaoTreeReducer.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        RunningJob rj = JobClient.runJob(job);
        return rj.isSuccessful() ? 0 : 1;
    }

    @Override
    public int run(String[] args) throws Exception {
        String input = args[0];
        String output = args[1];
        String config_path = args[2];
        numOfMappers = Integer.parseInt(args[3].trim());
        numOfReducers = Integer.parseInt(args[4].trim());
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(Constants.CONFIG_FILE_PATH, config_path);
        return runEtaoTreeJob(input, output, properties);
    }

    public static void main(String[] args) throws Exception {
        System.out.println(args.length);
        if (args.length < 5) {
            System.out.println("Invalid number of arguments");
            System.out.println(args.length);
            System.out.println("Usage : EffectEtaoTree [InputAccessPath] [OutputPath] [config_path]"
                    + "[numOfMappers] [numOfReducers]");
            System.exit(1);
        }

        int success = ToolRunner.run(new EffectETaoTreeRunner(), args);
        System.out.println("Status : " + success);
        System.exit(success);
    }
}
