package rank;
/*
 * Author @radha_krishna
 * Graph.java
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


        
public class Graph {
        
	/*
	 * Map class
	 */
 public static class Map extends Mapper<LongWritable, Text , Text, Text> {
       
	 /*
	  * map function for getting Graph Properties
	  * 
	  */
	 public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		 String line = value.toString();
		 StringTokenizer st = new StringTokenizer(line);
		 String temp = Integer.toString(st.countTokens()-1);
		 
		 //Emit value for each node
		 context.write(new Text("node"),new Text(temp));

	 }
 } 
 
 /*
  * Reducer class
  */

 public static class Reduce extends Reducer<Text, Text, Text, Text> {
/*
 * Reduce function
 * 
 */
    public void reduce(Text key, Iterable<Text> values, Context context) 
    		throws IOException, InterruptedException {
    	/*
    	 * initializations
    	 */
    	int noOfNodes = 0;
    	int minDegree=Integer.MAX_VALUE;
    	int maxDegree=Integer.MIN_VALUE;
    	int sum = 0;
    	int count=0;
    	// Iterate through values
    	for (Text val : values) {
    		noOfNodes++;
    		int temp = Integer.parseInt(val.toString());
    		sum += temp;	

    		if(temp<minDegree){
    			//compare with min degree
    			minDegree=temp;
    		}
    		if(temp>maxDegree){
    			//compare with max degree
    			maxDegree=temp;
    		}
    		//count the number of nodes
    		count++;
    	}

    	// Write the properties to context
    	context.write(new Text("number of edges "), new Text(Integer.toString(sum)));
    	context.write(new Text("number of nodes "), new Text(Integer.toString(count)));
    	context.write(new Text("Minimum out-degree  "), new Text(Integer.toString(minDegree)));
    	context.write(new Text("Maximum out-degree  "), new Text(Integer.toString(maxDegree)));

    	//Average degree calculation
    	float avgCost = 0;
    	if(noOfNodes!=0){
    		avgCost = (float) ((sum)/noOfNodes);
    	}
    	context.write(new Text("Average out-degree  "), new Text(Float.toString(avgCost)));

}
 }
        
 /*
  * Job for getting Graph properties
  */
 
 public void GraphPropertiesJob(String input,String output) throws IOException, InterruptedException, ClassNotFoundException{
	 Configuration conf = new Configuration();
	    /*
	     * Job for calculating graph properties   
	     */
	    Job job = new Job(conf, "graphproperties");
	    // set key , value classes for output
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    job.setJarByClass(Graph.class);
	    
	    
	    FileInputFormat.addInputPath(job, new Path(input));
	    FileOutputFormat.setOutputPath(job, new Path(output));
	    
	    job.waitForCompletion(true);
 }
 
 
      

}