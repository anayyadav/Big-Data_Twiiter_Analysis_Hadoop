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

/**
 *
 * @author anay
 */
public class TimeAnalysis 
{
    
    public static class TimeMapper extends Mapper<Object, Text, Text, IntWritable> 
    {
        private Text data = new Text();
        private final IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            // Format per tweet is id;date;hashtags;tweet;
            String tweets = value.toString();
            if(StringUtils.ordinalIndexOf(tweets,";",4)>-1)
            {
                int startIndex = StringUtils.ordinalIndexOf(tweets,";",1) + 1;
                int finishIndex = StringUtils.ordinalIndexOf(tweets, ";", 2);
                //split by ',' and take the first element (2012-07-27, 20:48:57, BST)
                String tweet_date = tweets.substring(startIndex,finishIndex).split(", ")[0];
                data.set(tweet_date);
                context.write(data, one);
            }
        }   
    }
    
    public static class TimeReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
    {

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
        {

            int sum = 0;

            for (IntWritable value : values) 
            {

                sum = sum + value.get(); 
            }

            context.write(key, new IntWritable(sum)); 
        }
    }
    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Time analysis");
        job.setJarByClass(TimeAnalysis.class);
        job.setMapperClass(TimeMapper.class);
        job.setCombinerClass(TimeReducer.class);
        job.setReducerClass(TimeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/home/anay/Desktop/hadoop_temp/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/anay/Desktop/hadoop_temp/Outputs/timeAnalysis"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}

 