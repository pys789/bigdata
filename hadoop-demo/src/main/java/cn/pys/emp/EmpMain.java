package cn.pys.emp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class EmpMain {
    public static void main(String[] args) throws Exception {
        //1、创建一个任务
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(EmpMain.class);

        //2、指定任务的map和map输出的数据类型
        job.setMapperClass(EmpMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(EmpVO.class);
        job.setPartitionerClass(EmpPartitioner.class);
        job.setNumReduceTasks(3);
        //3、指定任务的reduce和reduce的输出数据的类型
        job.setReducerClass(EmpReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(EmpVO.class);

        //4、指定任务的输入路径、任务的输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        //执行任务
        job.waitForCompletion(true);
    }
}
