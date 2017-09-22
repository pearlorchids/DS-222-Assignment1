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

public class NaiveBayes {


// Word tokenizer 
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
      String[] array = value.toString().split("\t");
      if(array.length>1)
      {
	String [] labels = array[0].trim().split(",");
	String textDocTemp1 = array[1].replace("\\\""," ");
	//String [] textDocTemp = textDocTemp1.split(" ");
	String [] textDocTemp = textDocTemp1.split("\"");
	String [] itwords = textDocTemp[1].replaceAll("[^a-zA-Z ]", "").toLowerCase().split(" ");
	//String [] itwords = textDocTemp[1].toLowerCase().split(" ");
	for(int i=0;i<labels.length;i++)
	{
		for(int j=0;j<itwords.length;j++)
		{
			//String label = labels[i].trim();
			word.set("X="+itwords[j]+"^Y="+labels[i]);
			context.write(word, one);
		}
	}

	
       
      }
    }
  }
  
  // Word tokenizer 
   public static class MapperIdentity extends Mapper<Object, Text, Text, Text>{
private Text word = new Text();
	private Text word2 = new Text();
	
    private final static IntWritable one = new IntWritable(1);
    
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
      String[] array = value.toString().split("\t");
      if(array.length>1)
      {
	String labels = array[0].trim();
	String textDocTemp1 = array[1].replace("\\\""," ");
	//String [] textDocTemp = textDocTemp1.split(" ");
	String [] textDocTemp = textDocTemp1.split("\"");
	String itwords = textDocTemp[1].replaceAll("[^a-zA-Z ]", "").toLowerCase();
	//String [] itwords = textDocTemp[1].toLowerCase().split(" ");
		//String label = labels[i].trim();
			word.set(labels+"~"+itwords);
			word2.set("1");
			context.write(word2,word);
	
	
       
      }
    }
  }
  
  // Tokenizer for each label 
  public static class TokenizerMapperLabels
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      //StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
      
      String[] array = value.toString().split("\t");
      if(array.length>1)
      {
	String [] labels = array[0].trim().split(",");
	String textDocTemp1 = array[1].replace("\\\""," ");
	//String [] textDocTemp = textDocTemp1.split(" ");
	String [] textDocTemp = textDocTemp1.split("\"");
	String [] itwords = textDocTemp[1].replaceAll("[^a-zA-Z ]", "").toLowerCase().split(" ");
	//String [] itwords = textDocTemp[1].toLowerCase().split(" ");
	for(int i=0;i<labels.length;i++)
	{
		for(int j=0;j<itwords.length;j++)
		{
			//String label = labels[i].trim();
			word.set("Y="+labels[i]+"^X=ANY");
			context.write(word, one);
		}
	}
	for(int i=0;i<labels.length;i++)
	{
			//String label = labels[i].trim();
			word.set("Y=ANY");
			context.write(word, one);
			word.set("Y="+labels[i]);
			context.write(word, one);
		
	}
       
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
       
  
  public static class IdentityReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();
    private Text result2 = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int id=1;
      
      for (Text val : values) {
	      String valstr = val.toString();
	      String [] array = valstr.split("~");
	      String [] labels = array[0].trim().split(",");
	       //String str = new String("hello world");
	       //String [] itwords = str.split(" ");
	String [] itwords = array[1].split(" ");
	
	for(int i=0;i<labels.length;i++)
	{
		for(int j=0;j<itwords.length;j++)
		{
			
			//String res = "~id"+id+","+labels[i];
			//result.set("~id"+id+","+labels[i]);
			result.set(valstr);
				//result2.set(itwords[j]);
				result2.set("~~~~~~");
			 context.write(result2,result);
		
		}
	}
	id++;
     
    }
  }
       }

  public static void main(String[] args) throws Exception {
	  
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "random");
    job.setJarByClass(NaiveBayes.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
    
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "random");
    job2.setJarByClass(NaiveBayes.class);
    job2.setMapperClass(TokenizerMapperLabels.class);
    job2.setCombinerClass(IntSumReducer.class);
    job2.setReducerClass(IntSumReducer.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job2, new Path(args[0]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    job2.waitForCompletion(true);
    
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "random");
    job3.setJarByClass(NaiveBayes.class);
    job3.setMapperClass(MapperIdentity.class);
    job3.setCombinerClass(IdentityReducer.class);
    job3.setReducerClass(IdentityReducer.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(args[0]));
    FileOutputFormat.setOutputPath(job3, new Path(args[3]));
    System.exit(job3.waitForCompletion(true) ? 0 : 1);
    
  }
}
