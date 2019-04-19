package app;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TFIDF {

    public static class TFIDFMapper extends Mapper<Object, Text, Text, DoubleWritable> {

        private static final String[] NAMES = {"金庸", "梁羽生", "李凉", "古龙", "卧龙生"};
        private static final int[] TOTAL_BOOKS_NUM = {15, 38, 41, 70, 54};

        private Text authorAndTerm = new Text();

        private int[] tfs = new int[5];
        private int[] booksNum = new int[5];

        private void reset() {
            for (int i = 0; i < 5; i++) {
                tfs[i] = 0;
                booksNum[i] = 0;
            }
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            reset();

            String[] items = value.toString().split("\t");
            String term = items[0];
            String[] booksAndTimes = items[1].substring(items[1].indexOf(",") + 1).split(";");
            for (String oneBookAndTimes : booksAndTimes) {
                String[] bookOrTimes = oneBookAndTimes.split(":");
                String bookTitle = bookOrTimes[0];
                int times = Integer.valueOf(bookOrTimes[1]);

                int index;
                if (bookTitle.startsWith("金庸")) {
                    index = 0;
                } else if (bookTitle.startsWith("梁羽生")) {
                    index = 1;
                } else if (bookTitle.startsWith("李凉")) {
                    index = 2;
                } else if (bookTitle.startsWith("古龙")) {
                    index = 3;
                } else if (bookTitle.startsWith("卧龙生")) {
                    index = 4;
                } else {
                    throw new RuntimeException();
                }
                tfs[index] += times;
                booksNum[index]++;
            }

            for (int i = 0; i < 5; i++) {
                double tfIdf = tfs[i] * Math.log(((double) TOTAL_BOOKS_NUM[i]) / (1 + booksNum[i]));

                authorAndTerm.set(String.format("%s,%s", NAMES[i], term));
                context.write(authorAndTerm, new DoubleWritable(tfIdf));
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        //以下配置均参考自官方文档
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "tf idf");
        job.setJarByClass(TFIDF.class);
        job.setMapperClass(TFIDF.TFIDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}