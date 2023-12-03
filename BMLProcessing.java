import java.io.IOException;

import javax.naming.Context;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class BProcessing {

    public static class BProcessingMapper extends Mapper<Object, Text, Text, IntWritable> { 

        private final static IntWritable zero = new IntWritable(0);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();

            boolean include = true;

            String newline = new String("");

            if (line.contains("tt")) {
                int loc = line.lastIndexOf("tt");

                String id = line.substring(loc + 2);

                try {
                    int imdb_id = Integer.parseInt(id);
                    int score = Integer.parseInt(line.substring(5, 6));
                    int newscore = 0;

                    if (score == 3) {
                        newscore = 1;
                    } else {
                        newscore = 0;
                    }

                    newline = line.substring(5, 6) + "," + String.valueOf(newscore) + "," + line.substring(loc + 2) + ",";

                } catch (NumberFormatException e) {
                    include = false;
                }

            } else {
                include = false;
            }

            if (include) {
                context.write(new Text(newline), zero);
            }
            
        }
    }

    public static class BProcessingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
    
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                result.set(0);
                context.write(key, result);
            }
    }
    
    
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
            System.err.println("Usage: BProcessing Data <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job(); 
        job.setJarByClass(BProcessing.class); 
        job.setJobName("BProcessing Bechdel Dataset");

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);
        job.setMapperClass(BProcessingMapper.class);
        job.setReducerClass(BProcessingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}