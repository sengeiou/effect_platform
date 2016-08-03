package com.etao.data.ep.accesstree;

import java.util.HashMap;
import java.util.Map;

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

import com.etao.lz.dw.util.TextPair;
import com.etao.lz.dw.util.TextPairKeyPartitioner;

public class AplusAccessTreeRunner extends Configured implements Tool {

	private int numOfMappers = 1;
	private int numOfReducers = 100;
	private boolean isTextFile = false;

	public AplusAccessTreeRunner() {
	}

	public AplusAccessTreeRunner(boolean isTextFile) {
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
			SequenceFileOutputFormat.setOutputCompressionType(job,
					SequenceFile.CompressionType.BLOCK);
			SequenceFileOutputFormat.setOutputCompressorClass(job,
					DefaultCodec.class);
		}
	}

	@Override
	public int run(String[] args) throws Exception {
		String pvInput = args[0];
		String iframeInput = args[1];
		String output = args[2];
		numOfMappers = Integer.parseInt(args[3].trim());
		numOfReducers = Integer.parseInt(args[4].trim());

		Map<String, String> properties = new HashMap<String, String>();
		if (args.length > 5) {
			for (int i = 5; i < args.length; i++) {
				String arg = args[i];
				int p = arg.indexOf("=");
				if (p != -1) {
					properties.put(arg.substring(0, p), arg.substring(p + 1));
				} else {
					System.out.println("Invalid parameter: " + arg);
				}
			}
		}

		JobConf job = new JobConf(getConf(), AplusAccessTreeMapper.class);
		job.setJobName("AplusAccessTreeBuilder");

		initJob(job);
		for (String propertyKey : properties.keySet()) {
			String value = properties.get(propertyKey);
			job.set(propertyKey, value);
		}

		MultipleInputs.addInputPath(job, new Path(pvInput),
				isTextFile ? TextInputFormat.class
						: SequenceFileInputFormat.class,
						AplusAccessTreeMapper.class);
		MultipleInputs.addInputPath(job, new Path(iframeInput),
				isTextFile ? TextInputFormat.class
						: SequenceFileInputFormat.class,
						AplusAccessTreeMapper.class);
		
		job.setReducerClass(AplusAccessTreeReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(output));

		RunningJob rt = JobClient.runJob(job);
		return rt.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.out.println(args.length);
		if (args.length < 5) {
			System.out.println("Invalid number of arguments");
			System.out.println(args.length);
			System.out
					.println("Usage : AplusAccessTreeRunner [InputPvPath] [InputIframePath] [OutputPath]"
							+ "[numOfMappers] [numOfReducers]");
			System.exit(1);
		}

		int success = ToolRunner.run(new AplusAccessTreeRunner(), args);
		System.out.println("Status : " + success);
		System.exit(success);
	}
}
