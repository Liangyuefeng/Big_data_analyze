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
	  private Text word = new Text();
	  private String pattern = "[^a-zA-Z']";       //pattern for split the numbers or special symbol

	  public void map(Object key, Text value, Context context
	                  ) throws IOException, InterruptedException {
		String line = value.toString();
	  	line = line.replaceAll(pattern, " ");     
	    StringTokenizer itr = new StringTokenizer(line);
	    while (itr.hasMoreTokens()) {
	      word.set(itr.nextToken());
	      context.write(word, one);
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
	  Job job = Job.getInstance(conf, "word count");
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


