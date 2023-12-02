import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class CountRecords {

    public static class CountRecordsMapper extends Mapper<Object, Text, Text, IntWritable> { 

        private final static IntWritable one = new IntWritable(1);
        
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            context.write(new Text("Total record count: "), one);
            
        }
    }

    public static class CountRecordsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
    
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
            }
    }
    
    
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
            System.err.println("Usage: Count Records <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job(); 
        job.setJarByClass(CountRecords.class); 
        job.setJobName("Count Bad Short Records in AirBnB Dataset");

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);
        job.setMapperClass(CountRecordsMapper.class);
        job.setReducerClass(CountRecordsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}