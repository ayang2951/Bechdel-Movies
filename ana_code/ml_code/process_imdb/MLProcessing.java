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

public class MLProcessing {

    public static class MLProcessingMapper extends Mapper<Object, Text, Text, IntWritable> { 

        private final static IntWritable zero = new IntWritable(0);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] cols = line.split(",");

            boolean include = true;
            String newline = new String("");

            // if line is empty, don't include it
            if (line.length() == 0) {
                include = false;
            }

            // make sure the year value is proper
            // remove the quotes
            if (cols[1].length() != 6) {
                include = false;
            } else {
                cols[1] = cols[1].substring(1, 5);
            }

            // parse the date and remove the year
            if (cols[2].length() != 10) {
                include = false;
            } else {
                cols[2] = cols[2].substring(5);
            }

            // make sure column is not "N/A"
            if (cols[9].length() == 0) {
                include = false;
            } else if (cols[9].contains("N")) {
                include = false;
            }

            // make sure column is not "N/A"
            if (cols[10].length() == 0) {
                include = false;
            } else if (cols[10].contains("N")) {
                include = false;
            }
            
            // make sure column is not "N/A"
            if (!(cols[11].contains("N"))) {
                int ll = cols[11].length();
                if (ll != 0) {
                    cols[11] = cols[11].substring(3, ll - 1);
                } else {
                    include = false;
                }
                
            }

            // process the box office data -- remove the | delimiter
            // remove all excess values, make the value numeric
            if (cols[12].contains("N")) {
                include = false;

            } else {
                cols[12] = cols[12].replace("|", "");
                cols[12] = cols[12].substring(2);
                int stop = cols[12].indexOf("\"");

                if (stop != -1) {
                    cols[12] = cols[12].substring(0, stop);
                }

                try {
                    int boxoffice = Integer.parseInt(cols[12]);
                    if (cols[12].length() == 0) {
                        include = false;
                    }

                } catch (NumberFormatException e) {
                    include = false;
                }
            }

            // get only the columns we want to analyze in the models
            if (include) {
                newline += cols[1];
                newline += ",";
                newline += cols[9];
                newline += ",";
                newline += cols[10];
                newline += ",";
                newline += cols[11];
                newline += ",";
                newline += cols[12];
                newline += ",";

                context.write(new Text(newline), zero);
            }
        }
    }

    public static class MLProcessingReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
    
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                result.set(0);
                context.write(key, result);
            }
    }
    
    
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
            System.err.println("Usage: MLProcessing Data <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job(); 
        job.setJarByClass(MLProcessing.class); 
        job.setJobName("MLProcessing Bechdel Dataset");

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);
        job.setMapperClass(MLProcessingMapper.class);
        job.setReducerClass(MLProcessingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}