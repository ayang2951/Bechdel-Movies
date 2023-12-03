import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class Processing {

    public static class ProcessingMapper extends Mapper<Object, Text, Text, IntWritable> { 

        private final static IntWritable zero = new IntWritable(0);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] cols = line.split(",");

            boolean include = true;
            String newline = new String("");

            if (line.length() == 0) {
                include = false;
            }

            if (cols[1].length() != 6) {
                include = false;
            } else {
                cols[1] = cols[1].substring(1, 5);
            }

            if (cols[2].length() != 10) {
                include = false;
            } else {
                cols[2] = cols[2].substring(5);
            }

            if (cols[9].length() == 0) {
                include = false;
            } else if (cols[9].contains("N")) {
                include = false;
            }

            if (cols[10].length() == 0) {
                include = false;
            } else if (cols[10].contains("N")) {
                include = false;
            }
            
            if (!(cols[11].contains("N"))) {
                int ll = cols[11].length();
                cols[11] = cols[11].substring(3, ll - 1);
            }

            if (!(cols[12].contains("N"))) {
                cols[12] = cols[12].replace("|", "");
                cols[12] = cols[12].substring(2);
                int stop = cols[12].indexOf("\"");

                if (stop != -1) {
                    cols[12] = cols[12].substring(0, stop);
                }
            }

            if (include) {
                newline += cols[1];
                newline += ",";
                newline += cols[9];
                newline += ",";
                newline += cols[10];
                newline += ",";
                newline += cols[11];
                newline += ",";

                context.write(new Text(newline), zero);
            }
        }
    }

    public static class ProcessingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
    
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                result.set(0);
                context.write(key, result);
            }
    }
    
    
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
            System.err.println("Usage: Processing Data <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job(); 
        job.setJarByClass(Processing.class); 
        job.setJobName("Processing Bechdel Dataset");

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);
        job.setMapperClass(ProcessingMapper.class);
        job.setReducerClass(ProcessingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}