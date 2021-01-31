package cn.pys.emp;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class EmpReducer extends Reducer<IntWritable, EmpVO, IntWritable, EmpVO> {
    @Override
    protected void reduce(IntWritable key3, Iterable<EmpVO> values3, Context context) throws IOException, InterruptedException {
        for (EmpVO vo : values3) {
            context.write(key3, vo);
        }
    }
}
