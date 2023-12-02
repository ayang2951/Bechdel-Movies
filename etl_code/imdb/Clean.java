import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Clean {

    public static class CleanMapper extends Mapper<Object, Text, Text, IntWritable> { 

        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            int quotes = 0;
            String newline = new String(line);

            int len = line.length();

            for (int i = 0; i < len; i++) {
                if (line.charAt(i) == '\"') {
                    quotes++;
                }

                if (line.charAt(i) == ',') {
                    if (quotes % 2 == 1) {
                        newline = newline.substring(0, i) + "|" + newline.substring(i + 1);
                    }
                }
            }
            context.write(new Text(newline), one);
            
        }
    }

    public static class CleanReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
    
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                result.set(0);
                context.write(key, result);
            }
    }
    
    
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
            System.err.println("Usage: Cleaning Data <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job(); 
        job.setJarByClass(Clean.class); 
        job.setJobName("Cleaning Bechdel Dataset");

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(CleanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}