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

        Table table = connection.getTable(TableName.valueOf("t1"));

        Scan scan = new Scan();
//        scan.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("a"));
        ResultScanner scanner = table.getScanner(scan);

        try {
            for (Result r = scanner.next(); r != null; r = scanner.next()){
                System.out.println(Bytes.toString(r.getRow()));
                System.out.println(Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("a"))));
            }
        }finally {
            scanner.close();
        }


    }
}
