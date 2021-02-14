import cn.pys.wordcount.WordCountMapper;
import cn.pys.wordcount.WordCountReducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MRUnitTest {

    @Test
    public void testMapper() throws IOException {
        WordCountMapper mapper = new WordCountMapper();
        MapDriver<LongWritable, Text, Text, IntWritable> driver = new MapDriver<>(mapper);
        driver.withInput(new LongWritable(1), new Text("I love Beijing"));
        driver.withOutput(new Text("I"), new IntWritable(1))
                .withOutput(new Text("love"), new IntWritable(1))
                .withOutput(new Text("Beijing"), new IntWritable(1));
        driver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        WordCountReducer reducer = new WordCountReducer();
        ReduceDriver<Text, IntWritable, Text, IntWritable> driver = new ReduceDriver<>(reducer);
        List<IntWritable> value3 = new ArrayList<>();
        value3.add(new IntWritable(1));
        value3.add(new IntWritable(1));
        value3.add(new IntWritable(1));

        driver.withInput(new Text("Hello"), value3);
        driver.withOutput(new Text("Hello"), new IntWritable(3));
        driver.runTest();
    }

    @Test
    public void testJob() throws IOException {
        WordCountMapper mapper = new WordCountMapper();
        WordCountReducer reducer = new WordCountReducer();

        MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> driver
                = new MapReduceDriver<>(mapper, reducer);
        driver.withInput(new LongWritable(1), new Text("I love China"))
                .withInput(new LongWritable(4), new Text("I love Beijing"));
        driver.withOutput(new Text("Beijing"), new IntWritable(1))
                .withOutput(new Text("China"), new IntWritable(1))
                .withOutput(new Text("I"), new IntWritable(2))
                .withOutput(new Text("love"), new IntWritable(2));
        driver.runTest();

    }
}
