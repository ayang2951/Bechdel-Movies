import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.util.ArrayList;
import java.util.Comparator;

public class RatingsStats {

    public static class RatingsStatsMapper extends Mapper<Object, Text, Text, DoubleWritable> { 

        private final static DoubleWritable one = new DoubleWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] cols = line.split(",");
            // int length = cols[1].length();

            try {

                double rating = Double.parseDouble(cols[9]);
                int year = Integer.parseInt(cols[1].substring(1, 5));

                if (rating > 0 && rating <= 10) {
                    DoubleWritable val = new DoubleWritable(rating);
                    context.write(new Text(String.valueOf(year) + ": "), val);
                }

            } catch (NumberFormatException e) {
                // context.write(new Text("uh oh"), one);
            }
            // context.write(new Text(cols[1] + "  " + cols[9] + "  " + String.valueOf(length) + "  "), one);
        }
    }

    public static class RatingsStatsReducer extends Reducer<Text, DoubleWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
    
            public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
                ArrayList<Double> vals = new ArrayList<>();          // for median
             
                double sum = 0;                                      // for mean

                ArrayList<Integer> modes = new ArrayList<>();       // for mode
                int floor = 0;         


                int i = 0;
                for (DoubleWritable value : values) {
                    vals.add(value.get());                          // for median

                    sum += value.get();                             // for mean

                    i += 1;
                }

                vals.sort(Comparator.naturalOrder());               // for median
                double median = vals.get(i / 2);

                double mean = sum / (double)i;                        // for mean

                for (int j = 0; j < vals.size(); j++) {
                    double val = vals.get(j);
                    if ((int)val > floor) {                         // for mode
                        modes.add(j);
                        floor += 1;
                    }
                }

                int k = 0;                                          // for mode
                int max = 0;
                int mode = 0;
                while (k < modes.size() - 1) {
                    if (modes.get(k + 1) - modes.get(k) > max) {
                        mode = k;
                        max = modes.get(k + 1) - modes.get(k);
                    }
                    k += 1;
                }

                mode = mode + 1;

                result.set(i);

                
                context.write(new Text(key + "Mean: " + String.valueOf(mean) + ", Median: " + String.valueOf(median) + ", Mode: " + String.valueOf(mode) + ", Count: "), result);
        
            }
    }
    
    
    public static void main(String[] args) throws Exception { 
        if (args.length != 2) {
            System.err.println("Usage: Getting statistics by year <input path> <output path>");
            System.exit(-1);
        }

        Job job = new Job(); 
        job.setJarByClass(RatingsStats.class); 
        job.setJobName("Profiling imdb Dataset");

        FileInputFormat.addInputPath(job, new Path(args[0])); 
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(1);
        job.setMapperClass(RatingsStatsMapper.class);
        job.setReducerClass(RatingsStatsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1); 
    }
}