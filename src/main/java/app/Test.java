package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Test {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(config);

        Table table = connection.getTable(TableName.valueOf("t"));

//        Admin admin = connection.getAdmin();
//        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf("wuxia"));
//        descriptor.addFamily(new HColumnDescriptor("count_family"));
//        admin.createTable(descriptor);

//        Get get = new Get(Bytes.toBytes("row1"));
//        Result result = table.get(get);
//        System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("a"))));

//        Put put = new Put(Bytes.toBytes("row2"));
//        put.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("a"), Bytes.toBytes("val from java"));
//        table.put(put);
//        System.out.println("put finish");


    }
}
