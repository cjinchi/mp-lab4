package app;

import jdk.management.resource.internal.inst.SocketOutputStreamRMHooks;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //以下配置均参考自官方文档
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "total order sorting");
        job.setJarByClass(TotalOrderSorting.class);
        job.setMapperClass(TotalOrderSorting.TotalOrderSortingMapper.class);
        job.setReducerClass(TotalOrderSorting.TotalOrderSortingReducer.class);
        job.setPartitionerClass(TotalOrderSorting.InvertedIndexPartitioner.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
