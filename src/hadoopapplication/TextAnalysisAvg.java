/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hadoopapplication;
import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import intintpair.IntIntPair;
/**
 *
 * @author anay
 */
public class TextAnalysisAvg 
{
    public static class TextMapper extends Mapper<Object, Text, Text, IntWritable> 
    {

        private Text emit_key = new Text("avg");
        private IntIntPair data = new IntIntPair();
        private final IntWritable one = new IntWritable(1);
        public void map(Object key, Text value,Reducer.Context context) throws IOException, InterruptedException
        {
          // Format per tweet is id;date;hashtags;tweet;
            String tweets = value.toString();
            if(StringUtils.ordinalIndexOf(tweets,";",4)>-1)
            {
                int startIndex = StringUtils.ordinalIndexOf(tweets,";",3) + 1;
                String tweet = tweets.substring(startIndex,tweets.lastIndexOf(';'));
                tweet = tweet.replaceAll("[^a-zA-Z0-9]+", "1");
                data.set(new IntWritable(tweet.length()), one);
                context.write(emit_key, data);
            }  
        }
    }

    public static class TextReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntIntPair> values, Context context) throws IOException, InterruptedException 
        {
            int sum_length = 0;
            int sum_tweets = 0;
            for (IntIntPair value : values) 
            {

                sum_length = sum_length + Integer.parseInt(value.getFirst().toString()); 
                sum_tweets = sum_tweets + Integer.parseInt(value.getSecond().toString()); 

            }
            result.set(sum_length/sum_tweets);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TextAvg");
        job.setJarByClass(TextAnalysisAvg.class);
        job.setMapperClass(TextMapper.class);
        job.setCombinerClass(TextReducer.class);
        job.setReducerClass(TextReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/home/anay/Desktop/hadoop_temp/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/anay/Desktop/hadoop_temp/Outputs/TextAnalysisAvg"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
