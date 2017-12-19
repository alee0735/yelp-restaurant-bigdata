package BigDataA2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Q1 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: Q1 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Q1");
        job.setJarByClass(Q1.class);

        job.setMapperClass(BusinessMap.class);
        job.setReducerClass(BusinessMap.Reduce.class);
        job.setCombinerClass(BusinessMap.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class BusinessMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String delims = "^";
            String[] businessData = StringUtils.split(value.toString(),delims);

            if (businessData.length ==3) {
                if(businessData[1].contains("Palo"))
                    context.write(new Text(businessData[1]), new IntWritable(1));
            }
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
        }

        public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {

            public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {

                int count=0;
                for(IntWritable t : values){
                    count++;
                }

                context.write(key,new IntWritable(count));

            }
        }
    }
}


