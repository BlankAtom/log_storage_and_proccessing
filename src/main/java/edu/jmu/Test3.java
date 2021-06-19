package edu.jmu;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


public class Test3  {
    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text>{
        IntWritable iw = new IntWritable(1);
        public static int index = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            String[] split = line.split("\\s+");

            if (split[0].equals("00:00:00")) {
                    context.write(new Text(line), new Text(""));
            }

            index ++;
        }
    }

    public static class MyReducer extends Reducer<Text,Text,Text,NullWritable>{

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }
//    public static void main(String[] args) throws Exception {
//        if( args.length < 2) {
//            System.out.println("Too least parameter!");
//            System.exit(1);
//        }
////        String filepath = "C:\\Users\\12451\\Documents\\Tencent Files\\1245197369\\FileRecv\\SogouQ.reduced";
//        Configuration conf = new Configuration();
//        BasicConfigurator.configure();
//        Job job = Job.getInstance(conf);
////        job.setJarByClass(Test3.class);
//        job.setJar("LogProccessing-1.0.jar");
//
//        job.setMapperClass(MyMapper.class);
//        job.setReducerClass(MyReducer.class);
//
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(Text.class);
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
//
//        FileInputFormat.setInputPaths(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job,new Path(args[1]));
//
//        boolean res = job.waitForCompletion(true);
//        System.exit(res?0:1);
//    }
}