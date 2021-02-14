package cn.pys.wordcount.emp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class EmpPartitioner extends Partitioner<IntWritable, EmpVO> {
    @Override
    public int getPartition(IntWritable k2, EmpVO empVO, int i) {
        if (k2.get() == 10) {
            return 1 % i;
        } else if (k2.get() == 20) {
            return 2 % i;
        } else {
            return 3 % i;
        }
    }
}
