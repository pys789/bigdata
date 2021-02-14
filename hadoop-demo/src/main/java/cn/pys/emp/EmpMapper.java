package cn.pys.emp;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class EmpMapper extends Mapper<LongWritable, Text, IntWritable, EmpVO> {
    @Override
    protected void map(LongWritable key1, Text value1, Context context) throws IOException, InterruptedException {
        // 7369,SMITH,CLERK,7902,1980/12/17,800,0,20
        EmpVO empVO = new EmpVO();
        String[] words = value1.toString().split(",");
        empVO.setEmpno(Integer.parseInt(words[0]));
        empVO.setEname(words[1]);
        empVO.setJob(words[2]);
        empVO.setMgr(Integer.parseInt(words[3]));
        empVO.setHiredate(words[4]);
        empVO.setSal(Integer.parseInt(words[5]));
        empVO.setComm(Integer.parseInt(words[6]));
        empVO.setDeptno(Integer.parseInt(words[7]));

        context.write(new IntWritable(empVO.getDeptno()), empVO);
    }
}
