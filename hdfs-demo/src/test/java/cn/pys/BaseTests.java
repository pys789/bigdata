package cn.pys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BaseTests {
    FileSystem fileSystem;

    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "192.168.112.111:9000");
        fileSystem = FileSystem.get(conf);

    }
}
