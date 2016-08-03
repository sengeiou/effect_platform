package com.etao.data.ep.ownership;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
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
import org.xml.sax.SAXException;

import com.etao.lz.effect.HoloConfig;
import com.etao.lz.effect.HoloConfig.Ind;
import com.etao.lz.effect.exception.HoloConfigParserException;
import com.etao.lz.dw.util.KeyAsDirNameOutputFormat;
import com.etao.lz.dw.util.TextPair;
import com.etao.lz.dw.util.TextPairKeyPartitioner;

public class EffectMRRunner extends Configured implements Tool {

	private int period = 1; // 归属有效周期
	private String tree_split = "none"; // 切分树规则

	private int map_num = 0; // map数
	private int reduce_num = 100; // reduce数
	private String config_paths = ""; // 配置文件
	private boolean isTextFile = false;

	EffectMRRunner() {

	}

	EffectMRRunner(boolean isTextFile) {
		this.isTextFile = isTextFile;
	}

	private void initJob(JobConf job) {
		job.setMapOutputKeyClass(TextPair.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setPartitionerClass(TextPairKeyPartitioner.class);
		job.setOutputValueGroupingComparator(TextPair.RealComparator.class);

		if (isTextFile) {
			job.setInputFormat(TextInputFormat.class);
		} else {
			job.setInputFormat(SequenceFileInputFormat.class);
		}

		job.setNumMapTasks(map_num);
		job.setNumReduceTasks(reduce_num);
		job.set("config_paths", config_paths);

		if (isTextFile) {
			job.setOutputFormat(KeyAsDirNameOutputFormat.KeyAsDirNameTextOutputFormat.class);
		} else {
			job.setOutputFormat(KeyAsDirNameOutputFormat.KeyAsDirNameSequenceFileOutputFormat.class);
			SequenceFileOutputFormat.setOutputCompressionType(job,
					SequenceFile.CompressionType.BLOCK);
			SequenceFileOutputFormat.setOutputCompressorClass(job,
					DefaultCodec.class);
		}
	}

	private int runTreeJob(String input, String output,
			Map<String, String> properties) throws IOException {
		JobConf job = new JobConf(getConf(), EffectMRRunner.class);
		job.setJobName("LzEpJstTreeBuilder");

		initJob(job);
		for (String propertyKey : properties.keySet()) {
			String value = properties.get(propertyKey);
			job.set(propertyKey, value);
		}

		job.setMapperClass(EffectNodeFinderMapper.class);
		job.setReducerClass(EffectNodeFinderReducer.class);

		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		RunningJob rt = JobClient.runJob(job);
		return rt.isSuccessful() ? 0 : 1;
	}

	private int runOwnerJob(List<String> access_inputs, String output,
			String gmv_input, String collect_input, String cart_input,
			Map<String, String> properties) throws IOException {

		JobConf job = new JobConf(getConf(), EffectMRRunner.class);
		job.setJobName("LzEpJstOwnership");

		initJob(job);
		for (String propertyKey : properties.keySet()) {
			String value = properties.get(propertyKey);
			job.set(propertyKey, value);
		}

		job.setReducerClass(EffectOwnershipReducer.class);
		FileOutputFormat.setOutputPath(job, new Path(output));
		for (String access_input : access_inputs) {
			MultipleInputs.addInputPath(job, new Path(access_input),
					isTextFile ? TextInputFormat.class
							: SequenceFileInputFormat.class,
					EffectOwnershipMapper.AccessMapper.class);
		}

		if (gmv_input != null && !gmv_input.isEmpty()) {
			MultipleInputs.addInputPath(job, new Path(gmv_input),
					isTextFile ? TextInputFormat.class
							: SequenceFileInputFormat.class,
					EffectOwnershipMapper.GmvMapper.class);
		}
		if (collect_input != null && !collect_input.isEmpty()) {
			MultipleInputs.addInputPath(job, new Path(collect_input),
					isTextFile ? TextInputFormat.class
							: SequenceFileInputFormat.class,
					EffectOwnershipMapper.CollectMapper.class);
		}
		if (cart_input != null && !cart_input.isEmpty()) {
			MultipleInputs.addInputPath(job, new Path(cart_input),
					isTextFile ? TextInputFormat.class
							: SequenceFileInputFormat.class,
					EffectOwnershipMapper.CartMapper.class);
		}

		RunningJob rt = JobClient.runJob(job);
		return rt.isSuccessful() ? 0 : 1;
	}

	/**
	 * 删除这些目录
	 * 
	 * @param paths
	 * @throws IOException
	 */
	public void rmHdfsDir(Path path) throws IOException {
		FileSystem fs = FileSystem.get(getConf());
		fs.delete(path, true);
		fs.close();
	}

	private void mvResultDir(String sourcePath, String targetPath) {
		try {
			FileSystem fs = FileSystem.get(getConf());

			if (!fs.exists(new Path(targetPath))) {
				if (!fs.mkdirs(new Path(targetPath))) {
					System.out.println("Unable to make directory: "
							+ targetPath);
				}
			}

			FileStatus[] fileStatusArray = fs.listStatus(new Path(sourcePath));
			if (fileStatusArray == null)
				return;
			for (FileStatus fileStatus : fileStatusArray) {
				if (fs.isFile(fileStatus.getPath())) {
					// DO SOMETHING YOU WANT
					// Ex. System.out.println()
				} else if (fs.isDirectory(fileStatus.getPath())) {
					mvResultDirs(fileStatus.getPath(), new Path(targetPath
							+ "/" + fileStatus.getPath().getName()));
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 转移src目录下的所有内容到dist目录下
	 * 
	 * @param src
	 * @param dist
	 */
	private void mvResultDirs(Path sourcePath, Path targetPath) {
		try {
			FileSystem fs = sourcePath.getFileSystem(getConf());

			fs.delete(targetPath, true);
			if (fs.exists(sourcePath)) {
				if (!fs.rename(sourcePath, targetPath)) {
					System.out.println("Unable to rename: " + sourcePath
							+ " to: " + targetPath);
				}
			} else if (!fs.mkdirs(targetPath)) {
				System.out.println("Unable to make directory: " + targetPath);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private boolean hasOutsideTradeInd(HoloConfig config) {
		ArrayList<Ind> inds = config.getEffect();
		for (Ind ind : inds) {
			if (ind.ind_id == 112 || ind.ind_id == 113 || ind.ind_id == 114
					|| ind.ind_id == 115 || ind.ind_id == 116
					|| ind.ind_id == 117) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Map<String, String> properties = new HashMap<String, String>();
		String input = arg0[0];
		String output = arg0[1];
		String mid = arg0[5];

		config_paths = arg0[2];
		map_num = Integer.parseInt(arg0[3]);
		reduce_num = Integer.parseInt(arg0[4]);

		// 解析配置文件
		List<HoloConfig> l_configs = new ArrayList<HoloConfig>();

		String gmv_input = null, collect_input = null, cart_input = null, outside_order = null;

		int runner_job = 0;

		if (arg0.length > 6) {
			for (int i = 6; i < arg0.length; i++) {
				String arg = arg0[i];
				int p = arg.indexOf("=");
				if (p != -1) {
					String key = arg.substring(0, p);
					String value = arg.substring(p + 1);
					if (key.equals("gmv")) {
						gmv_input = value;
					} else if (key.equals("collect")) {
						collect_input = value;
					} else if (key.equals("cart")) {
						cart_input = value;
					} else if (key.equals("outside_order")) {
						outside_order = value;
					} else if (key.equals("period")) {
						period = Integer.getInteger(value);
					} else if (key.equals("tree_split")) {
						tree_split = value;
					} else if (key.equals("runner_job")) {
						runner_job = Integer.parseInt(value);
					} else if (key.equals("files")) {
						for (String local_config_path : value.split(",")) {
							if (local_config_path.length() == 0) {
								continue;
							}
							HoloConfig config = new HoloConfig();
							try {
								config.loadFile(local_config_path);
							} catch (ParserConfigurationException e) {
								e.printStackTrace();
								return 1;
							} catch (SAXException e) {
								e.printStackTrace();
								return 1;
							} catch (IOException e) {
								e.printStackTrace();
								return 1;
							} catch (HoloConfigParserException e) {
								e.printStackTrace();
								return 1;
							}

							if (config.plan_id == 0) {
								System.out
										.println("Invalid parameter: 没有读取到配置文件");
								return 1;
							}
							l_configs.add(config);
						}
					} else {
						properties.put(key, value);
					}
				} else {
					System.out.println("Invalid parameter: " + arg);
					return 1;
				}
			}
		}

		if (l_configs.size() == 0) {
			System.out.println("没有添加files参数");
			return -1;
		}

		String mid_tmp = mid;
		String output_tmp = output;

		List<String> mid_paths = new ArrayList<String>();
		List<String> mid_paths_for_outside_order = new ArrayList<String>();
		// List<String> output_paths = new ArrayList<String>();

		List<Integer> plan_ids = new ArrayList<Integer>();
		for (HoloConfig config : l_configs) {
			plan_ids.add(config.plan_id);
			mid_tmp += "_" + config.plan_id;
			output_tmp += "_" + config.plan_id;

			// String output_path = output + '/' + config.plan_id;
			// output_paths.add(output_path);
		}

		if (runner_job == 0 || runner_job == 1 || runner_job == 2) {
			rmHdfsDir(new Path(mid_tmp));
			if (runTreeJob(input, mid_tmp, properties) == 1) {
				System.out.println("Tree Job Error!");
				return 1;
			}

			mvResultDir(mid_tmp, mid);
			rmHdfsDir(new Path(mid_tmp));

			// 后续MR任务仅处理不为空的mid_path
			FileSystem fs = FileSystem.get(getConf());
			FileStatus[] fileStatusArray = fs.listStatus(new Path(mid));
			if (fileStatusArray == null) {
				System.out.println("染色建树后没有输出");
				return 1;
			}
			for (FileStatus fileStatus : fileStatusArray) {
				if (fs.isDirectory(fileStatus.getPath())) {
					String pathName = fileStatus.getPath().getName();
                    int pathNameId = 0;
                    try {
                        pathNameId = Integer.valueOf(pathName).intValue();
                    } catch(Exception e) {
                        // set default vaule for _temporary path
                        pathNameId = -1;
                    }
					for (HoloConfig config : l_configs) {
						if (config.plan_id == pathNameId) {

							String mid_path = mid + '/' + pathName;
							mid_paths.add(mid_path);
							if (hasOutsideTradeInd(config)) {
								mid_paths_for_outside_order.add(mid_path);
							}
						}

					}
				}
			}

			/*
			 * for(Integer plan_id : plan_ids) { mvResultDirs(new Path(mid_tmp +
			 * '/' + plan_id), new Path(mid + '/' + plan_id)); }
			 */
		}
		if (runner_job == 0 || runner_job == 2) {
			rmHdfsDir(new Path(output_tmp));
			if (mid_paths.size() > 0) {
				if (runOwnerJob(mid_paths, output_tmp, gmv_input,
						collect_input, cart_input, properties) == 1) {
					System.out.println("Owner Job Error!");
					return 1;
				}
				mvResultDir(output_tmp, output + "/inside");
				rmHdfsDir(new Path(output_tmp));
			}

			mvResultDir(output_tmp, output + "/outside");
			rmHdfsDir(new Path(output_tmp));
			/*
			 * for(Integer plan_id : plan_ids) { mvResultDirs(new
			 * Path(output_tmp + '/' + plan_id), new Path(output + '/' +
			 * plan_id)); }
			 */
		}

		return 0;
	}

	public static void main(String[] args) throws Exception {
		System.out.println(args.length);
		if (args.length < 6) {
			System.out.println("Invalid number of arguments");
			System.out.println(args.length);
			System.out
					.println("Usage : EffectOwnership [InputAccessPath] [OutputPath] [config_paths]"
							+ "[numOfMappers] [numOfReducers] [mid_path]"
							+ "<gmv=GmvLogPath> <collect=CollectLogPath> <cart=CartLogPath> <outside_order=OutsideOrderPath>"
							+ "<period=1(归属周期，默认1)> <tree_split=none(默认)>"
							+ "<runner_job=1/2> <files=(本地路径)> <ROOT_IS_LP=true/false(树的根节点是不是全部认为是效果页, true为开启)>");
			System.exit(1);
		}

		int success = ToolRunner.run(new EffectMRRunner(), args);
		System.out.println("Status : " + success);
		System.exit(success);
	}
}
