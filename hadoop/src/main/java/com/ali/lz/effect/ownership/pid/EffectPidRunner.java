package com.ali.lz.effect.ownership.pid;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.hadooputils.TextPairKeyPartitioner;
import com.ali.lz.effect.utils.Constants;

public class EffectPidRunner extends Configured implements Tool {

    private int numOfMappers = 1;
    private int numOfReducers = 100;
    private boolean isTextFile = false;

    public EffectPidRunner() {
    }

    public EffectPidRunner(boolean isTextFile) {
        this.isTextFile = isTextFile;
    }

    private void initJob(JobConf job) {
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(BytesWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setPartitionerClass(TextPairKeyPartitioner.class);
        job.setOutputValueGroupingComparator(TextPair.RealComparator.class);
        job.setNumMapTasks(numOfMappers);
        job.setNumReduceTasks(numOfReducers);

        if (isTextFile) {
            job.setInputFormat(TextInputFormat.class);
        } else {
            job.setInputFormat(SequenceFileInputFormat.class);
        }
        if (isTextFile) {
            job.setOutputFormat(SequenceFileOutputFormat.class);
        } else {
            job.setOutputFormat(SequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        }
    }

    private int runTreeJob(String input, String output, Map<String, String> properties) throws IOException {
        JobConf job = new JobConf(getConf(), EffectPidRunner.class);
        job.setJobName("Lz_Effect_Platform_PidTreeBuilder");

        initJob(job);
        for (String propertyKey : properties.keySet()) {
            String value = properties.get(propertyKey);
            job.set(propertyKey, value);
        }

        job.setMapperClass(EffectPidTreeMapper.class);
        job.setReducerClass(EffectPidTreeReducer.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        RunningJob rt = JobClient.runJob(job);
        return rt.isSuccessful() ? 0 : 1;
    }

    private int runOwnerShipJob(String access_input, String output, String gmv_input, Map<String, String> properties)
            throws IOException {

        JobConf job = new JobConf(getConf(), EffectPidRunner.class);
        job.setJobName("Lz_Effect_Platform_PidOwnership");

        initJob(job);
        for (String propertyKey : properties.keySet()) {
            String value = properties.get(propertyKey);
            job.set(propertyKey, value);
        }

        job.setReducerClass(EffectPidOwnershipReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleInputs.addInputPath(job, new Path(access_input), isTextFile ? TextInputFormat.class
                : SequenceFileInputFormat.class, EffectPidOwnershipMapper.AccessMapper.class);

        if (gmv_input != null && !gmv_input.isEmpty()) {
            MultipleInputs.addInputPath(job, new Path(gmv_input), isTextFile ? TextInputFormat.class
                    : SequenceFileInputFormat.class, EffectPidOwnershipMapper.GmvMapper.class);
        }

        RunningJob rt = JobClient.runJob(job);
        return rt.isSuccessful() ? 0 : 1;
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        String input = args[0];
        String output = args[1];
        String config_path = args[2];
        numOfMappers = Integer.parseInt(args[3].trim());
        numOfReducers = Integer.parseInt(args[4].trim());
        String mid_path = args[5];
        String gmv_input = "";
        int runner_job = 0;
        String exec_date = "";
        if (args.length > 7) {
            for (int i = 6; i < args.length; i++) {
                String arg = args[i];
                int p = arg.indexOf("=");
                if (p != -1) {
                    String key = arg.substring(0, p);
                    String value = arg.substring(p + 1);
                    if (key.equals("gmv")) {
                        gmv_input = value;
                    } else if (key.equals("runner_job")) {
                        runner_job = Integer.parseInt(value);
                    } else if (key.equals("exec_date")) {
                        exec_date = value;
                    }
                } else {
                    System.out.println("Invalid parameter: " + arg);
                    return 1;
                }
            }
        }
        Map<String, String> properties = new HashMap<String, String>();
        properties.put(Constants.CONFIG_FILE_PATH, config_path);
        properties.put("exec_date", exec_date);

        if (runner_job == 0 || runner_job == 1)
            if (runTreeJob(input, mid_path, properties) == 1) {
                System.out.println("Pid Tree Job Error!");
                return 1;
            }
        if (runner_job == 0 || runner_job == 2)
            if (runOwnerShipJob(mid_path, output, gmv_input, properties) == 1) {
                System.out.println("Pid Ownership Job Error!");
                return 1;
            }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(args.length);
        if (args.length < 9) {
            System.out.println("Invalid number of arguments");
            System.out.println(args.length);
            System.out.println("Usage : EffectPidOwnership [InputAccessPath] [OutputPath] [config_path]"
                    + "[numOfMappers] [numOfReducers] [mid_path] <gmv=GmvLogPath> <runner_job=1/2> <exec_date=date>");
            System.exit(1);
        }

        int success = ToolRunner.run(new EffectPidRunner(), args);
        System.out.println("Status : " + success);
        System.exit(success);
    }
}
