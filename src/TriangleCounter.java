import java.io.IOException;
import java.util.StringTokenizer;
import java.lang.Integer;
import java.util.*;
import java.lang.Long;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleCounter {
  
  // public static class DuplicateMapper
  //   extends Mapper<Object, Text, Text, LongWritable>{

  //     public void map(Object key, Text value, Context context)
  //       throws IOException, InterruptedException {

  //         String[] user_follower = value.toString().trim().split("\\s+");

  //         if (user_follower.length > 1) {
  //           Long userID = Long.parseLong(user_follower[0]);
  //           Long followerID = Long.parseLong(user_follower[1]);
  //           Text key = new Text();
  //           private final LongWritable dummy = new LongWritable(-1);

  //           if (userID < followerID) {
  //             key = user_follower[0] + "," + user_follower[1];
  //           } else {
  //             key = user_follower[1] + "," + user_follower[0];
  //           }

  //           context.write(key, dummy);
  //         }
  //       }
  //   }

  // public static class DuplicateReducer
  //   extends Reducer<Text, LongWritable, LongWritable, LongWritable> {

  //     public void reduce(Text key, Iterable<LongWritable> values,
  //       Context context) throws IOException, InterruptedException {
  //         String[] nodes = key.toString().trim().split(",");

  //         LongWritable key = new LongWritable(Long.parseLong(nodes[0]));
  //         LongWritable value = new LongWritable(Long.parseLong(nodes[1]));

  //         context.write(key, value);
  //       }
  //   }

  public static class PairMapper
       extends Mapper<Object, Text, LongWritable, LongWritable>{
  
    public void map(Object key, Text value, Context context) 
        throws IOException, InterruptedException {

      String[] user_follower = value.toString().trim().split("\\s+");
      
      if (user_follower.length > 1) {
        Long userID = Long.parseLong(user_follower[0]);
        Long followerID = Long.parseLong(user_follower[1]);
        
        if (userID < followerID) {
          context.write(new LongWritable(userID), new LongWritable(followerID));
        } else {
          context.write(new LongWritable(followerID), new LongWritable(userID));
        }
      }
    }
  }

  public static class ComponentReducer
       extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable();

    private final LongWritable dollar = new LongWritable(-1);

    public void reduce(LongWritable key, Iterable<LongWritable> values,
      Context context) throws IOException, InterruptedException {
        
      
      Iterator<LongWritable> itr = values.iterator();
      Text newKey = new Text();
      Set<Long> set = new LinkedHashSet<>();
      while (itr.hasNext()) {
        // context.write(new Text(key.toString()), new LongWritable(itr.next().get()));
        set.add(itr.next().get());
      }
      ArrayList<Long> arrayList = new ArrayList<>();
      Iterator<Long> iter = set.iterator();
      while (iter.hasNext()) {
        Long item = new Long(iter.next());
        arrayList.add(item);
      }

      for (int i = 0; i < arrayList.size(); i++) {
        Long value1 = arrayList.get(i);
        String strValue1 = value1.toString();
        
        // Emit read key value pair
        // (key, value1), $
        newKey.set(key.toString() + ',' + strValue1);
        context.write(newKey, dollar);
        
        for (int j = i + 1; j < arrayList.size(); j++) {
          Long value2 = arrayList.get(j);
          String strValue2 = value2.toString();
          // Emit triad from read key value pair
          newKey.set(strValue1 + "," + strValue2);
          int compare = value1.compareTo(value2);
          // context.write(newKey, new LongWritable());
          if (compare < 0) {
            newKey.set(strValue1 + "," + strValue2);
            context.write(newKey, key);
          } else {
            newKey.set(strValue2 + "," + strValue1);
            context.write(newKey, key);
          }
        }
      }
    }
  }

  public static class ComponentMapper
      extends Mapper<Object, Text, Text, LongWritable>{
  
    public void map(Object key, Text value, Context context) 
      throws IOException, InterruptedException {

        String[] key_value = value.toString().trim().split("\\s+");
        if (key_value.length > 1) {
          context.write(new Text(key_value[0]), new LongWritable(Long.parseLong(key_value[1])));
        }
        
    }
  }

  public static class TriangleReducer
    extends Reducer<Text, LongWritable, Text, LongWritable> {
    
    long count;
    boolean hasDollar;

    public void reduce(Text key, Iterable<LongWritable> values,
        Context context) throws IOException, InterruptedException {

      final Text RESULT_STRING = new Text("result");
      
      Iterator<LongWritable> itr = values.iterator();
      count = 0;
      hasDollar = false;
      
      while (itr.hasNext()) {
        Long value = itr.next().get();
        if (value != -1) {
          count += 1;
        } else {
          hasDollar = true;
        }
      }

      if (hasDollar) {
        context.write(RESULT_STRING, new LongWritable(count));
      }
    }
  }

  public static class AggregateReducer
    extends Reducer<Text, LongWritable, Text, LongWritable> {

    public void reduce(Text key, Iterable<LongWritable> values,
        Context context) throws IOException, InterruptedException {
    
      long sum = 0;
      Iterator<LongWritable> itr = values.iterator();

      while (itr.hasNext()) {
        Long value = itr.next().get();
        sum += value;
      }

      context.write(key, new LongWritable(sum));
        
    }
  }



  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Triangle Component Generator");
    job.setJarByClass(TriangleCounter.class);
    job.setMapperClass(PairMapper.class);
    job.setReducerClass(ComponentReducer.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path("/user/ve/component"));

    Configuration counterConf = new Configuration();
    Job counterJob = Job.getInstance(counterConf, "Triangle Counter");
    counterJob.setJarByClass(TriangleCounter.class);
    counterJob.setMapperClass(ComponentMapper.class);
    counterJob.setReducerClass(TriangleReducer.class);
    counterJob.setOutputKeyClass(Text.class);
    counterJob.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(counterJob, new Path("/user/ve/component"));
    FileOutputFormat.setOutputPath(counterJob, new Path("/user/ve/count"));

    Job aggregateJob = Job.getInstance(counterConf, "Aggregate Triangle");
    aggregateJob.setJarByClass(TriangleCounter.class);
    aggregateJob.setMapperClass(ComponentMapper.class);
    aggregateJob.setReducerClass(AggregateReducer.class);
    aggregateJob.setOutputKeyClass(Text.class);
    aggregateJob.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(aggregateJob, new Path("/user/ve/count"));
    FileOutputFormat.setOutputPath(aggregateJob, new Path(args[1]));

    int jobCompletion = job.waitForCompletion(true) ? 0 : 1;
    if (jobCompletion == 0)
      jobCompletion = counterJob.waitForCompletion(true) ? 0 : 1;
    if (jobCompletion == 0)
      jobCompletion = aggregateJob.waitForCompletion(true) ? 0 : 1;

    System.exit(jobCompletion);
  }
}