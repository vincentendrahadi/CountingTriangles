import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Integer;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleComponentGenerator {

  public static class PairMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{
  
    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {

      String[] user_follower = value.toString().trim().split("\\s+");
      
      if (user_follower.length > 1) {
        int userID = Integer.parseInt(user_follower[0]);
        int followerID = Integer.parseInt(user_follower[1]);
        if (userID < followerID) {
          context.write(new IntWritable(userID), new IntWritable(followerID));
        } else {
          context.write(new IntWritable(followerID), new IntWritable(userID));
        }
      }
    }
  }

  public static class ComponentReducer
       extends Reducer<IntWritable, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    private final IntWritable dollar = new IntWritable(-1);

    public void reduce(IntWritable key, Iterable<IntWritable> values,
        Context context) throws IOException, InterruptedException {

      Iterator<IntWritable> itr = values.iterator();
      Integer counter = 0;
      Text newKey = new Text();
      Text pairKey = new Text();
      String temp = "";
      
      while (itr.hasNext()) {
        counter++;
        String value = Integer.toString(itr.next().get());
        
        // Emit read key value pair
        newKey.set(key.toString() + ',' + value);
        context.write(newKey, dollar);
        
        // Emit triad from read key value pair
        if (counter == 2 ) {
          counter = 0;
          pairKey.set(temp + value);
          context.write(pairKey, key);
        } else {
          temp = value + ',';
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Triangle Component Generator");
    job.setJarByClass(TriangleComponentGenerator.class);
    job.setMapperClass(PairMapper.class);
    job.setReducerClass(ComponentReducer.class);
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}