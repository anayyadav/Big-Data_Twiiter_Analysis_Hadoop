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
public class TextAnalysis 
{
    public static class TMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private Text upperbound = new Text("0");
	private final IntWritable one = new IntWritable(1);
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
	{
	// Format per tweet is id;date;hashtags;tweet;
            String tweets = value.toString();
            if(StringUtils.ordinalIndexOf(tweets,";",4)>-1)
            {
                int startIndex = StringUtils.ordinalIndexOf(tweets,";",3) + 1;
                String tweet = tweets.substring(startIndex,tweets.lastIndexOf(';'));
                tweet = tweet.replaceAll("[^a-zA-Z0-9]+", "1");
                if (tweet.length()<=140)
                {
                        int upperb = (int) (Math.ceil((float)tweet.length()/5)*5);
                        int lowerb = (int) ((Math.ceil((float)tweet.length()/5)-1)*5 + 1);
                        upperbound.set(String.valueOf(lowerb) + "-"+ String.valueOf(upperb));
                }
                else
                {
                        upperbound.set(">140");
                }
                context.write(upperbound, one);
            }
	}  
    }

    public static class TReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "textAnalysis");
        job.setJarByClass(TextAnalysis.class);
        job.setMapperClass(TMapper.class);
        job.setCombinerClass(TReducer.class);
        job.setReducerClass(TReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("/home/anay/Desktop/hadoop_temp/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/anay/Desktop/hadoop_temp/Outputs/textAnalysis"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
