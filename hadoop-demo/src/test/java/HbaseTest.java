import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;

public class HbaseTest {

    private Configuration conf;

    @Before
    public void init() throws Exception {
        conf = new Configuration();
        conf.set("hbase.zookeeper.quorum", "192.168.112.111");
    }

    @Test
    public void testCreateTable() throws Exception {
        HBaseAdmin client = new HBaseAdmin(conf);
        HTableDescriptor mytable = new HTableDescriptor(TableName.valueOf("mytable"));
        mytable.addFamily(new HColumnDescriptor("info"));
        mytable.addFamily(new HColumnDescriptor("grade"));
        client.createTable(mytable);
        client.close();

    }

    @Test
    public void testPutData() throws Exception {
        HTable client = new HTable(conf, "mytable");
        Put put = new Put(Bytes.toBytes("id001"));

        put.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"),Bytes.toBytes("Tom"));
        client.put(put);
        client.close();

    }

    @Test
    public void testGetData() throws Exception {
        HTable client = new HTable(conf, "mytable");
        Get get = new Get(Bytes.toBytes("id001"));
        get.addColumn(Bytes.toBytes("info"),Bytes.toBytes("name"));
        Result result = client.get(get);

        byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
        System.out.println("名字是:"+Bytes.toString(value));
        client.close();
    }

    @Test
    public void testScanData() throws Exception {
        HTable client = new HTable(conf, "mytable");
        Scan scan = new Scan();
        //scan.setFilter(null);
        ResultScanner scanner = client.getScanner(scan);
        for(Result result:scanner){
            byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
            System.out.println("名字是:"+Bytes.toString(value));
        }
        client.close();
    }
}
