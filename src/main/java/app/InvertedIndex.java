package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {

        //类似于一个temp变量，用于把String转为Text
        private Text termAndFileName = new Text();

        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获取当前内容所在文件的文件名
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

            //分词，并将每个词记录为 <term#fileName,count>
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase());
            while (itr.hasMoreTokens()) {
                //教材中是用'#'作为分隔符，但term或filename内部可能会出现'#'字符，所以改用空格
                termAndFileName.set(String.format("%s %s", itr.nextToken(), fileName));
                context.write(termAndFileName, ONE);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text> {
        //类似于temp变量，用于把String转为Text
        private Text str1 = new Text();
        private Text str2 = new Text();

        /**
         * 由于一个key [即term+filename]调用一次reduce()函数，因此同一个term实际上需要调用多次reduce函数才能处理完毕。
         * currentTerm表示当前正在处理的term，当reduce函数中判断到term和currentTerm不相同时，说明已经开始处理下一个term了。
         * currentPostings表示currentTerm的索引，当currentTerm变化时需要清空。
         */
        private String currentTerm = null;

        private List<String> postings = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] items = key.toString().split(" ");
            String term = items[0];
            String fileName = items[1];
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            if (currentTerm == null || !currentTerm.equals(term)) {
                //输出旧term的信息
                writeCurrentTerm(context);
                postings.clear();
                //开始一个新term的处理
                currentTerm = term;
            }

            postings.add(String.format("<%s,%d>", fileName, sum));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            //由于遇到新term才会输出旧term的索引，因此最后一个term的索引需要在该函数中手动输出
            writeCurrentTerm(context);
        }

        private void writeCurrentTerm(Context context) throws IOException, InterruptedException {
            if (currentTerm == null){
                return;
            }

            StringBuilder postingsBuilder = new StringBuilder();
            for (String posting : postings) {
                postingsBuilder.append(posting);
                postingsBuilder.append(';');
            }
            if (postingsBuilder.length() > 0) {
                //删去最后一个分号
                postingsBuilder.deleteCharAt(postingsBuilder.length() - 1);
            }

            str1.set(currentTerm);
            str2.set(postingsBuilder.toString());
            context.write(str1, str2);
        }

    }

    public static class InvertedIndexCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        //类似于temp变量，用于将int转换为IntWritable
        private IntWritable total = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            total.set(sum);
            context.write(key, total);
        }
    }

    public static class InvertedIndexPartitioner extends HashPartitioner<Text, IntWritable> {
        private Text term = new Text();

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            term.set(key.toString().split(" ")[0]);
            return super.getPartition(term, value, numReduceTasks);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //以下配置均参考自官方文档
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndex.InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndex.InvertedIndexCombiner.class);
        job.setReducerClass(InvertedIndex.InvertedIndexReducer.class);
        job.setPartitionerClass(InvertedIndex.InvertedIndexPartitioner.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
