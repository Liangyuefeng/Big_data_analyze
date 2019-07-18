import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wc {
	 public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		 
  //IntWritable, Text is a class implemented in Hadoop to encapsulate Java data types. 
  //These classes implement the WritableComparable interface, which can be serialized to facilitate data exchange in a distributed environment. 
  private final static IntWritable one = new IntWritable(1);
  private String str = new String();
  private Text word = new Text();
  
  //void map(K1 key, V1 value, Context context)
  //Map a single input k/v pair to an intermediate k/v pair
  //Output pairs are not the same type as input pairs, and input pairs can be mapped to 0 or more output pairs. * Context: Collects the <k,v> pairs of the Mapper output.
  //Context write (k, v) method: add a (k, v) pair to the context
  //Mainly writes Map and Reduce functions. This Map function uses the StringTokenizer function to separate the strings, and saves the words into the word by the write method.
  //The write method stores the binary group (word, 1) into the context.
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
	StringTokenizer itr = new StringTokenizer(value.toString(),"  \t\n\r\f,:;?![]'");	
	Text doi = new Text();
    doi.set(itr.nextToken());

    if (itr.hasMoreTokens()) {
       word.set(itr.nextToken());	
       Text keynew = new Text();
       str = doi.toString()+":"+word.toString().replace("."," ");
       keynew.set(str);
       context.write(keynew, one);
    }
  }
}

public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
  private IntWritable result = new IntWritable();
  //The reduce method in the Reducer class:
  //void reduce(Text key, Iterable<IntWritable> values, Context context)
  //The k/v comes from the context in the map function, may be further processed (combiner), also output through context
  
  public void reduce(Text key, Iterable<IntWritable> values,
                     Context context
                     ) throws IOException, InterruptedException {
    int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
    }
    result.set(sum);
    context.write(key, result);
  }
}

public static void main(String[] args) throws Exception {
  //BasicConfigurator.configure();
  Configuration conf = new Configuration();
  Job job = Job.getInstance(conf, "csv");
  job.setJarByClass(wc.class);
  job.setMapperClass(TokenizerMapper.class);
  job.setCombinerClass(IntSumReducer.class);
  job.setReducerClass(IntSumReducer.class);
  job.setOutputKeyClass(Text.class);
  job.setOutputValueClass(IntWritable.class);
  FileInputFormat.addInputPath(job, new Path(args[0]));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));
  System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
