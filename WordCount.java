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

public class WordCount {
	
	
	  public static class TokenizerMapper
	       extends Mapper<Object, Text, Text, IntWritable>{

	    private final static IntWritable one = new IntWritable(1);
	    public void map(Object key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	     String allnew=value.toString().replaceAll("[^a-zA-Z\\s]","");
	    	StringTokenizer itr = new StringTokenizer(allnew);
	      while (itr.hasMoreTokens()) {
	    	  String word = itr.nextToken();
	    	  word=word.toLowerCase(); 
	    	  char firstChar = 0;
			  if(word.length()>0)
	    	  firstChar = word.charAt(0);
			  String a= String.valueOf(firstChar); 
	     	  context.write(new Text(a) , one);
	        }
	      }
	    }
	 
	  public static class IntSumReducer
      extends Reducer<Text,IntWritable,Text,IntWritable> {
   private IntWritable result = new IntWritable();

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

 @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
   Configuration conf = new Configuration();
   Job job = Job.getInstance(conf, "wordcount");
   job.setJarByClass(WordCount.class);
   job.setMapperClass(TokenizerMapper.class);
   job.setCombinerClass(IntSumReducer.class);
   job.setReducerClass(IntSumReducer.class);
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(IntWritable.class);
   FileInputFormat.addInputPath(job, new Path(args[0]));
   Path outputPath = new Path(args[1]);
   
   FileOutputFormat.setOutputPath(job, outputPath);
   outputPath.getFileSystem(conf).delete(outputPath);
   
   System.exit(job.waitForCompletion(true) ? 0 : 1);
 }

}