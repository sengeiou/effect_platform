package com.ali.lz.effect.ownership;

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

import com.ali.lz.effect.exception.HoloConfigParserException;
import com.ali.lz.effect.hadooputils.HdfsUtils;
import com.ali.lz.effect.hadooputils.KeyAsDirNameOutputFormat;
import com.ali.lz.effect.hadooputils.TextPair;
import com.ali.lz.effect.hadooputils.TextPairKeyPartitioner;
import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.holotree.HoloConfig.Ind;
import com.ali.lz.effect.utils.Constants;

public class EffectMRRunner extends Configured implements Tool {

    private int period = 1; // 归属有效周期
    private int mapNum = 0; // map数
    private int reduceNum = 100; // reduce数
    private String configPaths = ""; // 配置文件
    private boolean isTextFile = false;
    // 是否将传入的midpath直接作为归属效果依赖的染色访问树。用于从同一染色规则构建的访问树中利用多种归属规则计算效果的场景, 避免多次重复建树。
    private boolean use_midpath_as_tree = false;
    /**
     * 是否将不同plan的数据输出到同一目录(planId=0目录)下。
     * 如果任务需要读取大量xml计算，且不需并行读取多个(组)xml运行，建议开启该配置。
     * 避免因同一reduce数据输出到多个plan目录，导致任务产出大量小文件引发文件数配额超标、下游任务list输入文件OOM等诸多问题。
     * 目前该配置应用于天猫品牌站效果分析项目
     */
    private boolean mergePlanOutputPath = false;

    private int runner_job = 0;

    private String inputAccessLogPath = null;
    private String inputGmvLogPath = null;
    private String inputCollectLogPath = null;
    private String inputCartLogPath = null;
    private String inputOutsideOrderPath = null;
    private String inputHjljLogPath = null;
    private String outputHjljTagPath = null;
    private String holoTreePath = null;
    private String outputOwnershipPath = null;
    private Map<String, String> properties = new HashMap<String, String>();

    // 解析配置文件
    private List<HoloConfig> configList = new ArrayList<HoloConfig>();

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

        job.setNumMapTasks(mapNum);
        job.setNumReduceTasks(reduceNum);
        job.set(Constants.CONFIG_FILE_PATH, configPaths);

        if (isTextFile) {
            if (mergePlanOutputPath)
                job.setOutputFormat(KeyAsDirNameOutputFormat.MergePlanDirTextOutputFormat.class);
            else
                job.setOutputFormat(KeyAsDirNameOutputFormat.KeyAsDirNameTextOutputFormat.class);
        } else {
            if (mergePlanOutputPath)
                job.setOutputFormat(KeyAsDirNameOutputFormat.MergePlanDirSeqFileOutputFormat.class);
            else
                job.setOutputFormat(KeyAsDirNameOutputFormat.KeyAsDirNameSequenceFileOutputFormat.class);
            SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
            SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        }
    }

    private int runTreeJob(String input, String output, Map<String, String> properties) throws IOException {
        JobConf job = new JobConf(getConf(), EffectMRRunner.class);
        job.setJobName("Lz_Effect_Platform_EffectNodeFinder");
        job.set("mapred.child.java.opts", "-Xmx2048m");

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

    private int runOwnerJob(List<String> access_inputs, String output, String gmv_input, String collect_input,
            String cart_input, Map<String, String> properties) throws IOException {

        JobConf job = new JobConf(getConf(), EffectMRRunner.class);
        job.setJobName("Lz_Effect_Platform_EffectOwnership");
        job.set("mapred.child.java.opts", "-Xmx2048m");
        initJob(job);
        for (String propertyKey : properties.keySet()) {
            String value = properties.get(propertyKey);
            job.set(propertyKey, value);
        }

        job.setReducerClass(EffectOwnershipReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        for (String access_input : access_inputs) {
            MultipleInputs.addInputPath(job, new Path(access_input), isTextFile ? TextInputFormat.class
                    : SequenceFileInputFormat.class, EffectOwnershipMapper.HoloTreeMapper.class);
        }

        if (gmv_input != null && !gmv_input.isEmpty()) {
            job.set("gmv_ownership", "true");
            MultipleInputs.addInputPath(job, new Path(gmv_input), isTextFile ? TextInputFormat.class
                    : SequenceFileInputFormat.class, EffectOwnershipMapper.GmvMapper.class);
        }
        if (collect_input != null && !collect_input.isEmpty()) {
            job.set("collect_ownership", "true");
            MultipleInputs.addInputPath(job, new Path(collect_input), isTextFile ? TextInputFormat.class
                    : SequenceFileInputFormat.class, EffectOwnershipMapper.CollectMapper.class);
        }
        if (cart_input != null && !cart_input.isEmpty()) {
            job.set("cart_ownership", "true");
            MultipleInputs.addInputPath(job, new Path(cart_input), isTextFile ? TextInputFormat.class
                    : SequenceFileInputFormat.class, EffectOwnershipMapper.CartMapper.class);
        }

        RunningJob rt = JobClient.runJob(job);
        return rt.isSuccessful() ? 0 : 1;
    }

    private int runOutsideTradeOwnerJob(List<String> access_inputs, String output, String outside_order_input,
            Map<String, String> properties) throws IOException {

        JobConf job = new JobConf(getConf(), EffectMRRunner.class);
        job.setJobName("Lz_Effect_Platform_EffectOutsideTradeOwnership");
        job.set("mapred.child.java.opts", "-Xmx2048m");
        initJob(job);
        for (String propertyKey : properties.keySet()) {
            String value = properties.get(propertyKey);
            job.set(propertyKey, value);
        }

        job.setReducerClass(EffectOutsideTradeOwnershipReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(output));
        for (String access_input : access_inputs) {
            MultipleInputs.addInputPath(job, new Path(access_input), isTextFile ? TextInputFormat.class
                    : SequenceFileInputFormat.class, EffectOutsideTradeOwnershipMapper.AccessMapper.class);
        }
        if (outside_order_input != null && !outside_order_input.isEmpty()) {
            MultipleInputs.addInputPath(job, new Path(outside_order_input), isTextFile ? TextInputFormat.class
                    : SequenceFileInputFormat.class, EffectOutsideTradeOwnershipMapper.OutsideTradeMapper.class);
        }

        RunningJob rt = JobClient.runJob(job);
        return rt.isSuccessful() ? 0 : 1;
    }

    private int runHjljOwnershipJob(String access_input, String output, String hjlj_log_input,
            Map<String, String> properties) throws IOException {
        JobConf job = new JobConf(getConf(), EffectMRRunner.class);
        job.setJobName("Lz_Effect_Platform_EffectHjljOwnership");
        job.set("mapred.child.java.opts", "-Xmx2048m");
        initJob(job);

        job.setMapOutputValueClass(Text.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);
        for (String propertyKey : properties.keySet()) {
            String value = properties.get(propertyKey);
            job.set(propertyKey, value);
        }

        job.setReducerClass(EffectHjljOwnershipReducer.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        MultipleInputs.addInputPath(job, new Path(access_input), isTextFile ? TextInputFormat.class
                : SequenceFileInputFormat.class, EffectHjljOwnershipMapper.AccessLogMapper.class);
        if (hjlj_log_input != null && !hjlj_log_input.isEmpty()) {
            MultipleInputs.addInputPath(job, new Path(hjlj_log_input), isTextFile ? TextInputFormat.class
                    : SequenceFileInputFormat.class, EffectHjljOwnershipMapper.HjljLogMapper.class);
        }

        RunningJob rt = JobClient.runJob(job);
        return rt.isSuccessful() ? 0 : 1;
    }

    private boolean hasOutsideTradeInd(HoloConfig config) {
        ArrayList<Ind> inds = config.getEffect();
        for (Ind ind : inds) {
            if (ind.ind_id == 112 || ind.ind_id == 113 || ind.ind_id == 114 || ind.ind_id == 115 || ind.ind_id == 116
                    || ind.ind_id == 117) {
                return true;
            }
        }
        return false;
    }

    private int initJobArguments(String[] args) {

        inputAccessLogPath = args[0];
        outputOwnershipPath = args[1];
        configPaths = args[2];
        mapNum = Integer.parseInt(args[3]);
        reduceNum = Integer.parseInt(args[4]);
        holoTreePath = args[5];

        if (args.length > 6) {
            for (int i = 6; i < args.length; i++) {
                String arg = args[i];
                int p = arg.indexOf("=");
                if (p != -1) {
                    String key = arg.substring(0, p);
                    String value = arg.substring(p + 1);
                    if (key.equals("gmv")) {
                        inputGmvLogPath = value;
                    } else if (key.equals("collect")) {
                        inputCollectLogPath = value;
                    } else if (key.equals("cart")) {
                        inputCartLogPath = value;
                    } else if (key.equals("outside_order")) {
                        inputOutsideOrderPath = value;
                    } else if (key.equals("hjlj")) {
                        inputHjljLogPath = value;
                    } else if (key.equals("hjlj_output")) {
                        outputHjljTagPath = value;
                    } else if (key.equals("period")) {
                        period = Integer.getInteger(value);
                    } else if (key.equals("runner_job")) {
                        runner_job = Integer.parseInt(value);
                    } else if (key.equals("use_midpath_as_tree")) {
                        use_midpath_as_tree = Boolean.parseBoolean(value);
                    } else if (key.equals("merge_plan_output_path")) {
                        mergePlanOutputPath = Boolean.parseBoolean(value);
//                        properties.put("mergePlanOutputPath", value);
                    } else if (key.equals("files")) {
                        for (String local_config_path : value.split(",")) {
                            if (local_config_path.length() == 0 || !local_config_path.endsWith(".xml")) {
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
                                System.out.println("Invalid parameter: 没有读取到配置文件");
                                return 1;
                            }
                            configList.add(config);
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

        if (configList.size() == 0) {
            System.out.println("没有添加files参数");
            return 1;
        }

        return 0;

    }

    @Override
    public int run(String[] args) throws Exception {

        if (initJobArguments(args) != 0) {
            System.err.println("initialize input job arguments error!");
            return 1;
        }
        String mid_tmp = holoTreePath;
        String output_tmp = outputOwnershipPath;

        boolean doHjljOwnership = false;
        if (inputHjljLogPath != null && outputHjljTagPath != null) {
            doHjljOwnership = true;
        }
        if (!mergePlanOutputPath) {
            for (HoloConfig config : configList) {
                mid_tmp += "_" + config.plan_id;
                output_tmp += "_" + config.plan_id;
            }
        } else {
            // 避免因直接使用output目录时，导致执行mvResultDir无法将output/* mv到output/inside/*
            output_tmp += "_" + Constants.MERGE_PLANID;
        }

        // 任务1：Matcher+构建访问树+染色
        if (runner_job == 0 || runner_job == 1) {
            HdfsUtils.rmHdfsDir(new Path(mid_tmp), getConf());
            // 黄金令箭效果标记
            if (doHjljOwnership) {
                HdfsUtils.rmHdfsDir(new Path(outputHjljTagPath), getConf());
                if (runHjljOwnershipJob(inputAccessLogPath, outputHjljTagPath, inputHjljLogPath, properties) == 1) {
                    System.err.println("HjljOwnership Job Error!");
                    return 1;
                }
                inputAccessLogPath = outputHjljTagPath;
            }
            if (runTreeJob(inputAccessLogPath, mid_tmp, properties) == 1) {
                System.err.println("Tree Job Error!");
                return 1;
            }

            if (!mergePlanOutputPath) {
                HdfsUtils.mvResultDir(mid_tmp, holoTreePath, getConf());
                HdfsUtils.rmHdfsDir(new Path(mid_tmp), getConf());
            }
            if (doHjljOwnership) {
                HdfsUtils.rmHdfsDir(new Path(outputHjljTagPath), getConf());
            }
        }

        List<String> treePaths = new ArrayList<String>();

        // 任务2: 归属站内成交、购物车、收藏及站外成交效果
        if (runner_job == 0 || runner_job == 2) {
            HdfsUtils.rmHdfsDir(new Path(output_tmp), getConf());
            if (setTreePathForOwnership(treePaths, 1) != 0) {
                System.err.println("set holo tree path error!");
                return 1;
            }
            if (treePaths.size() > 0) {
                if (runOwnerJob(treePaths, output_tmp, inputGmvLogPath, inputCollectLogPath, inputCartLogPath,
                        properties) == 1) {
                    System.err.println("Owner Job Error!");
                    return 1;
                }
                HdfsUtils.mvResultDir(output_tmp, outputOwnershipPath + "/inside", getConf());
                HdfsUtils.rmHdfsDir(new Path(output_tmp), getConf());
            }

            if (setTreePathForOwnership(treePaths, 2) != 0) {
                System.err.println("set holo tree path error!");
                return 1;
            }
            if (treePaths.size() > 0) {
                if (runOutsideTradeOwnerJob(treePaths, output_tmp, inputOutsideOrderPath, properties) == 1) {
                    System.out.println("Outside Trade Owner Job Error!");
                    return 1;
                }
            }

            HdfsUtils.mvResultDir(output_tmp, outputOwnershipPath + "/outside", getConf());
            HdfsUtils.rmHdfsDir(new Path(output_tmp), getConf());
        }

        return 0;
    }

    public int setTreePathForOwnership(List<String> treePaths, int ownershipType) throws IOException {
        // 效果归属MR任务仅处理不为空的mid_path
        treePaths.clear();
        FileSystem fs = FileSystem.get(getConf());
        FileStatus[] fileStatusArray = fs.listStatus(new Path(holoTreePath));
        if (fileStatusArray == null) {
            System.err.println("没有访问树染色数据");
            return 1;
        }

        if (mergePlanOutputPath) {
            String mid_path = holoTreePath + '/' + Constants.MERGE_PLANID;
            if (ownershipType == 1) {
                treePaths.add(mid_path);
            } else if (ownershipType == 2) {
                for (HoloConfig config : configList) {
                    if (hasOutsideTradeInd(config)) {
                        treePaths.add(mid_path);
                    }
                }
            }
            return 0;
        }
        if (use_midpath_as_tree) {
            if (ownershipType == 1) {
                treePaths.add(holoTreePath);
            } else if (ownershipType == 2) {
                for (HoloConfig config : configList) {
                    if (hasOutsideTradeInd(config)) {
                        treePaths.add(holoTreePath);
                    }
                }
            }
        } else {
            for (FileStatus fileStatus : fileStatusArray) {
                if (fs.isDirectory(fileStatus.getPath())) {
                    String pathName = fileStatus.getPath().getName();
                    for (HoloConfig config : configList) {
                        if (config.plan_id == Integer.valueOf(pathName).intValue()) {

                            String mid_path = holoTreePath + '/' + pathName;
                            if (ownershipType == 1) {
                                treePaths.add(mid_path);
                            } else if (ownershipType == 2) {
                                if (hasOutsideTradeInd(config)) {
                                    treePaths.add(mid_path);
                                }
                            }
                        }

                    }
                }
            }
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.out.println("Invalid number of arguments");
            System.out.println(args.length);
            System.out.println("Usage : EffectOwnership [InputAccessPath] [OutputPath] [config_paths] "
                    + "[numOfMappers] [numOfReducers] [mid_path] "
                    + "<gmv=GmvLogPath> <collect=CollectLogPath> <cart=CartLogPath> "
                    + "<outside_order=OutsideOrderPath> <hjlj=HjljLogPath> <hjlj_output=HjljTagOutputPath>"
                    + "<period=1(归属周期，默认1)> <runner_job=0/1/2> "
                    + "<files=(本地路径)> <use_midpath_as_tree=true/false(是否直接将传入的midpath直接作为归属效果依赖的染色访问树，默认为false)> "
                    + "<merge_plan_output_path=true/false(是否将任务输入的所有xml plan结果数据输出到同一目录, 默认为false)");
            System.exit(1);
        }

        int success = ToolRunner.run(new EffectMRRunner(), args);
        System.out.println("Status : " + success);
        System.exit(success);
    }
}
