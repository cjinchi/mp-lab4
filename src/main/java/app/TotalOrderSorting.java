package app;

import jdk.management.resource.internal.inst.SocketOutputStreamRMHooks;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.StringTokenizer;

public class TotalOrderSorting {
    public static class TotalOrderSortingMapper extends Mapper<Object, Text, DoubleWritable, Text>{
        private Text term = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString().substring(0,value.find(",")));
            term.set(tokenizer.nextToken());
            DoubleWritable times = new DoubleWritable(Double.valueOf(tokenizer.nextToken()));
            context.write(times,term);
        }
    }

    public static class TotalOrderSortingReducer extends Reducer<DoubleWritable,Text, Text, DoubleWritable>{
        private Text term = new Text();
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            term = values.iterator().next();
            context.write(term,key);
        }
    }

    public static class InvertedIndexPartitioner extends HashPartitioner<DoubleWritable,Text>{
        @Override
        public int getPartition(DoubleWritable key, Text value, int numReduceTasks) {
            //TODO:根据倒排索引的结果分布，确定如何分类
            return 0;
        }
    }

    public static void main(String[] args) {
        Text text = new Text();
        String str = "test  123.456789,金庸:12";
        text.set(str);
        StringTokenizer tokenizer = new StringTokenizer(text.toString().substring(0,text.find(",")));
        String term = tokenizer.nextToken();
        double times = Double.valueOf(tokenizer.nextToken());
        System.out.println(term);
        System.out.println(times);
    }
}
