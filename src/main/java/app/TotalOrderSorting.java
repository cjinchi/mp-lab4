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
        private String temp;

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString().substring(0,value.toString().indexOf(",")));
            term.set(tokenizer.nextToken());
            DoubleWritable times = new DoubleWritable(Double.valueOf(tokenizer.nextToken()));
            context.write(times,term);
        }
    }

    public static class TotalOrderSortingReducer extends Reducer<DoubleWritable,Text, Text, DoubleWritable>{
        @Override
        public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text term:values){
                context.write(term,key);
            }

        }
    }

    public static class InvertedIndexPartitioner extends HashPartitioner<DoubleWritable,Text>{
        @Override
        public int getPartition(DoubleWritable key, Text value, int numReduceTasks) {
            double val = key.get();
            if (val<1.0001){
                return 0;
            }else if (val<1.2){
                return 1;
            }else if(val<1.3){
                return 2;
            }else if(val<1.4){
                return 3;
            }else if (val<1.6){
                return 4;
            }else if(val<1.8){
                return 5;
            }else if(val<2.0){
                return 6;
            }else if(val<2.3){
                return 7;
            }else if(val<3){
                return 8;
            }else if(val<5){
                return 9;
            }else if(val<9){
                return 10;
            }else{
                return 11;
            }
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
