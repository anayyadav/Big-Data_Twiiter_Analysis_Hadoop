package hadoopapplication;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang.StringUtils;

public class HashtagAnalysis 
{

    public static class HMapper extends Mapper<Object, Text, Text, IntWritable> 
    {

        private final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value,Context context) throws IOException, InterruptedException
        {
            String tweets = value.toString();
            //if there is a 4th semicolon...
            if(StringUtils.ordinalIndexOf(tweets,";",4)>-1)
            {

                int startIndex = StringUtils.ordinalIndexOf(tweets,";",2) + 1;
                String tweet = tweets.substring(startIndex,tweets.lastIndexOf(';'));
                tweet = tweet.toLowerCase();
                Matcher matcher = Pattern.compile("#go\\s*(\\w+)").matcher(tweet);

                while(matcher.find())
                {
                    try
                    {

                        String team = tweet.substring(matcher.start() + 3, matcher.start() + 7);
                        if(team.equals("team"))
                        {
                            context.write(new Text(tweet.substring(matcher.start() + 7, matcher.end())), one);
                            break;
                        }

                    } 
                    catch(StringIndexOutOfBoundsException e)
                    {

                    }
                    context.write(new Text(matcher.group(1)), one);

                }

                matcher = Pattern.compile("#team\\*s*(\\w+)").matcher(tweet);

                while(matcher.find())
                {
                        context.write(new Text(matcher.group(1)), one);

                }
            }    
            
        }
    }

    public static class HReducer extends Reducer<Text, IntWritable, Text, IntWritable> 
    {

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException 
        {
            int sum = 0;

            for (IntWritable value : values) 
            {
                sum = sum+ value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hashtag");
        job.setJarByClass(HashtagAnalysis.class);
        job.setMapperClass(HMapper.class);
        job.setCombinerClass(HReducer.class);
        job.setReducerClass(HReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/home/anay/Desktop/hadoop_temp/input"));
        FileOutputFormat.setOutputPath(job, new Path("/home/anay/Desktop/hadoop_temp/Outputs/hashtagAnalysis"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}