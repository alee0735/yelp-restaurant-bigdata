package BigDataA2;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class Q5 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();		// get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: Q5 <in> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Q5");
        job.setJarByClass(Q5.class);
        job.setMapperClass(Q5.BusinessMap.class);
        job.setReducerClass(Q5.Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class BusinessMap extends Mapper<LongWritable, Text, Text, DoubleWritable> {


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String delims = "^";
            String[] businessData = StringUtils.split(value.toString(), delims);
            String stars = businessData[3];

            if (businessData.length == 4) {
                context.write(new Text(businessData[2]), new DoubleWritable(Double.parseDouble(stars)));
            }
        }

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
        }
    }

    public static class Reduce extends Reducer<Text,DoubleWritable,Text,Text> {
        private Map<Text, DoubleWritable> countMap = new HashMap<Text,DoubleWritable>();

        public void reduce(Text key, Iterable<DoubleWritable> values,Context context ) throws IOException, InterruptedException {
            double count = 0;
            double sum = 0;
            for (DoubleWritable val : values) {
                count++;
                sum += val.get();
            }
            countMap.put(new Text(key), new DoubleWritable(sum / count));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, DoubleWritable> sortedMap = sortByValues(countMap);

            ArrayList<Text> sortedList = new ArrayList<Text>();

            for(Text key : sortedMap.keySet()) {
                sortedList.add(key);
            }

            Collections.reverse(sortedList);

            int counter = 0;
            for (Text key : sortedList) {
                if (counter++ == 10) {
                    break;
                }
                context.write(key, new Text(sortedMap.get(key).toString()));
            }
        }
    }

    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}
