package com.ali.lz.effect.ownership;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.xml.sax.SAXException;

import com.ali.lz.effect.exception.HoloConfigParserException;
import com.ali.lz.effect.holotree.HoloConfig;
import com.ali.lz.effect.utils.Constants;
import com.ali.lz.effect.utils.StringUtil;

public class EffectAdClick extends Configured implements Tool {

    public static class MyMapper extends MapReduceBase implements Mapper<Object, Text, Text, IntWritable> {

        private HashMap<Integer, Integer> plan_map = new HashMap<Integer, Integer>();
        private static int ACCESS_LOG_COLUMN_NUM = 21;

        public void configure(JobConf conf) {
            String conf_files = conf.get("config_paths");
            for (String conf_file : conf_files.split(",")) {
                if (conf_file.length() == 0) {
                    continue;
                }
                HoloConfig config = new HoloConfig();
                try {
                    config.loadFile(conf_file);
                } catch (ParserConfigurationException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (SAXException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (HoloConfigParserException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                int plan_id = config.plan_id;
                int user_id = config.analyzer_id;
                plan_map.put(plan_id, user_id);
            }
        }

        @Override
        public void map(Object key, Text value, OutputCollector<Text, IntWritable> output, Reporter result)
                throws IOException {
            String line = value.toString();
            String[] fields = line.split(Constants.CTRL_A, -1);
            // 判断输入字段长度
            if (fields.length < ACCESS_LOG_COLUMN_NUM) {
                return;
            }

            String adid = fields[14];
            String cookie = fields[5];

            for (Map.Entry<Integer, Integer> p_entry : plan_map.entrySet()) {
                int plan_id = p_entry.getKey();
                int analyzer_id = p_entry.getValue();

                String[] in = { String.valueOf(analyzer_id), String.valueOf(plan_id), adid, cookie };
                output.collect(new Text(StringUtil.join(in, Constants.CTRL_A)), new IntWritable(1));
            }
        }
    }

    public static class MyReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter result)
                throws IOException {
            String str_key = key.toString();
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(new Text(""), new Text(str_key + Constants.CTRL_A + String.valueOf(sum)));
        }

    }

    @Override
    public int run(String[] arg0) throws Exception {
        Configuration conf = getConf();
        JobConf job = new JobConf(conf, EffectAdClick.class);
        job.setJobName("Lz_Effect_Platform_EffectAdClick");

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setInputFormat(SequenceFileInputFormat.class);
        job.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);

        FileInputFormat.addInputPath(job, new Path(arg0[0]));
        FileOutputFormat.setOutputPath(job, new Path(arg0[1]));
        job.setNumMapTasks(Integer.parseInt(arg0[2]));
        job.setNumReduceTasks(Integer.parseInt(arg0[3]));
        job.set("config_paths", arg0[4]);

        // 设置基本参数外的配置
        if (arg0.length > 5) {
            for (int i = 5; i < arg0.length; i++) {
                String arg = arg0[i];
                int p = arg.indexOf("=");
                if (p != -1) {
                    job.set(arg.substring(0, p), arg.substring(p + 1));
                } else {
                    System.out.println("Invalid parameter: " + arg);
                }
            }
        }

        RunningJob rt = JobClient.runJob(job);
        return rt.isSuccessful() ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.out.println("Invalid number of arguments");
            System.out.println(args.length);
            System.out
                    .println("Usage : EffectAdClick [InputPath] [OutputPath] [numOfMappers] [numOfReducers] [config_paths]");
            System.exit(1);
        }

        int success = ToolRunner.run(new EffectAdClick(), args);
        System.out.println("Status : " + success);
        System.exit(success);
    }
}
