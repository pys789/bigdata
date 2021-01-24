import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestHdfs {

    private FileSystem fileSystem;

    @Before
    public void init() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "192.168.112.111:9000");
        fileSystem = FileSystem.get(conf);
    }

    @Test
    public void testMakeDir() throws IOException {
        fileSystem.mkdirs(new Path("/folder4"));
        fileSystem.close();
    }

    @Test
    public void testUpload() throws IOException {
        FileInputStream inputStream = new FileInputStream("E:\\tar\\hadoop-2.7.3.tar.gz");
        FSDataOutputStream outputStream = fileSystem.create(new Path("/folder1/hadoop-2.7.3.tar.gz"));
        // 使用固定写法
        /*byte[] bytes = new byte[1024];
        int len;
        while ((len = inputStream.read(bytes)) > 0) {
            outputStream.write(bytes, 0, len);
        }
        outputStream.flush();
        inputStream.close();
        outputStream.close();*/
        // 使用工具类
        IOUtils.copyBytes(inputStream, outputStream, 1024);
        fileSystem.close();
    }

    @Test
    public void testDownload() throws IOException {
        FSDataInputStream inputStream = fileSystem.open(new Path("/folder1/hadoop-2.7.3.tar.gz"));
        FileOutputStream outputStream = new FileOutputStream("E:\\tar\\hadoop-2.7.3-download.tar.gz");
        IOUtils.copyBytes(inputStream, outputStream, 1024);
        fileSystem.close();
    }

    @Test
    public void testDataNode() throws IOException {
        DistributedFileSystem distributedFileSystem = (DistributedFileSystem) this.fileSystem;
        DatanodeInfo[] dataNodeStats = distributedFileSystem.getDataNodeStats();
        for (DatanodeInfo info : dataNodeStats) {
            System.out.println(info.getHostName() + "\t" + info.getName());
        }
        this.fileSystem.close();
    }

}
