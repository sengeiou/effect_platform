package com.ali.lz.effect.ownership.wirelssclient;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
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

public class EffectClientRunner extends Configured implements Tool {

    private int numOfMappers = 1;
    private int numOfReducers = 100;
    private boolean isTextFile = false;

    public EffectClientRunner() {
    }

    public EffectClientRunner(boolean isTextFile) {
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

    private int runJob(String access_input, String gmv_input, String output) throws IOException {

        JobConf job = new JobConf(getConf(), EffectClientRunner.class);
        job.setJobName("Lz_Effect_Platform_Client_Ownership");

        initJob(job);

        job.setReducerClass(EffectClientReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleInputs.addInputPath(job, new Path(access_input), isTextFile ? TextInputFormat.class
                : SequenceFileInputFormat.class, EffectClientMapper.AccessMapper.class);

        if (gmv_input != null && !gmv_input.isEmpty()) {
            MultipleInputs.addInputPath(job, new Path(gmv_input), isTextFile ? TextInputFormat.class
                    : SequenceFileInputFormat.class, EffectClientMapper.GmvMapper.class);
        }

        RunningJob rt = JobClient.runJob(job);
        return rt.isSuccessful() ? 0 : 1;
    }

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        String input = args[0];
        String output = args[1];
        numOfMappers = Integer.parseInt(args[2].trim());
        numOfReducers = Integer.parseInt(args[3].trim());
        String gmv_input = "";
        if (args.length > 4) {
            for (int i = 4; i < args.length; i++) {
                String arg = args[i];
                int p = arg.indexOf("=");
                if (p != -1) {
                    String key = arg.substring(0, p);
                    String value = arg.substring(p + 1);
                    if (key.equals("gmv")) {
                        gmv_input = value;
                    }
                } else {
                    System.out.println("Invalid parameter: " + arg);
                    return 1;
                }
            }
        }

        if (runJob(input, gmv_input, output) == 1) {
            System.out.println("Client Ownership Job Error!");
            return 1;
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(args.length);
        if (args.length < 5) {
            System.out.println("Invalid number of arguments");
            System.out.println(args.length);
            System.out.println("Usage : EffectWirelessOwnership [InputAccessPath] [OutputPath] "
                    + "[numOfMappers] [numOfReducers] <gmv=GmvLogPath> ");
            System.exit(1);
        }

        int success = ToolRunner.run(new EffectClientRunner(), args);
        System.out.println("Status : " + success);
        System.exit(success);
    }
}