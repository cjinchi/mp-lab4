package util;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class HBaseController {
    private static final String TABLE_NAME = "wuxia1";
    private static final String COLUMN_FAMILY_NAME = "count_family";
    private static final byte[] COLUMN_FAMILY_BYTES = Bytes.toBytes(COLUMN_FAMILY_NAME);
    private static final byte[] COLUMN_BYTES = Bytes.toBytes("average_count");
    //lab4: HBase connection
    private static Connection connection;
    private static Table table;

    static {
        try {
            connection = ConnectionFactory.createConnection(HBaseConfiguration.create());

            Admin admin = connection.getAdmin();
            if (!admin.tableExists(TableName.valueOf(TABLE_NAME))){
                HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
                descriptor.addFamily(new HColumnDescriptor(COLUMN_FAMILY_NAME));
                admin.createTable(descriptor);
            }

            table = connection.getTable(TableName.valueOf(TABLE_NAME));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void addAverageCount(String term,double averageCount) throws IOException {
        Put put = new Put(Bytes.toBytes(term));
        put.addColumn(COLUMN_FAMILY_BYTES, COLUMN_BYTES, Bytes.toBytes(averageCount));
        table.put(put);
    }

    public static void saveToLocalFile(String filename) throws IOException {
        Writer writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filename), StandardCharsets.UTF_8));

        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        try {
            for (Result r = scanner.next(); r != null; r = scanner.next()){
                String key = Bytes.toString(r.getRow());
                String value = Bytes.toString(r.getValue(COLUMN_FAMILY_BYTES,COLUMN_BYTES));

                String result = String.format("%s\t%s\n",key,value);
                writer.write(result);
            }
        }finally {
            scanner.close();
            writer.close();
        }
    }
}
