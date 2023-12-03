import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class UniqueRecsMapper
            extends Mapper<Object, Text, Text, NullWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String [] arr = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            String col = "Title: "+ arr[0] + " Year " + arr[1] + " Bechdel Score " + arr[2] + " Dubious Score "+ arr[3];
            Text columns = new Text(col);
            context.write(columns,NullWritable.get());

            
        }
}
