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

import java.io.IOException;
import java.util.StringTokenizer;

public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

        //类似于一个temp变量，用于把String转为Text
        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获取当前内容所在文件的文件名
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            // 获取所在行的偏移值
            String offset = key.toString();
            //拼接为 fileName#offset
            Text fileNameAndOffset = new Text(String.format("%s#%s", fileName, offset));

            //分词，并将每个词记录为 <word,file_name#offset>
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, fileNameAndOffset);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        //类似于一个temp变量，用于把String转为Text
        private Text str = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //用于单词key的所有索引
            StringBuilder indexs = new StringBuilder();

            for (Text index : values) {
                indexs.append(index.toString());
                indexs.append(";");
            }

            //删去最后一个分号
            indexs.deleteCharAt(indexs.length() - 1);

            str.set(indexs.toString());
            context.write(key, str);
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndex.InvertedIndexMapper.class);
        job.setCombinerClass(InvertedIndex.InvertedIndexReducer.class);
        job.setReducerClass(InvertedIndex.InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
