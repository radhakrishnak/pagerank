package rank;
/*
 * Author @radha_krishna
 * PageRank.java
 */
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/*
 * Class with pagerank implementation
 */
public class PageRank {
	
	// Enum for job counter(maintains the number of iterations)
	public static enum JOB_COUNTER {
		outOfIterations;
	};

	/*
	 * Mapper class for calculating pagerank
	 */
	public static class PRankMap extends Mapper<LongWritable, Text, Text, Text> {

		private Text nodeID = new Text();

		/*
		 * Map function
		 * 
		 */
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line);

			//get the node
			nodeID.set(st.nextToken());

			//parse the initialized rank previously
			float rank = new Float(st.nextToken().toString());
			int numTokens = st.countTokens();
			
			//calculate the rank for each of the tokens(destination vertices)
			// for next iteration
			float rankEach = 0.00F;
			if (numTokens == 0) {
				rankEach = rank;
			} else {
				rankEach = rank / numTokens;
			}
			String emitStr = Float.toString(rank)+" ";

			while (st.hasMoreTokens()) {
				Text nxtToken = new Text(st.nextToken());
				emitStr += nxtToken;
				emitStr += " ";
				// add a "$" symbol to the end of emit which consists of rank
				context.write(nxtToken, new Text(Float.toString(rankEach)+"$"));
			}
			
			//do not add anything for emit without rank
			//emit the first node with remaining tokens(destinations)
			context.write(nodeID, new Text(emitStr));

		}

	}

	/*
	 * Reducer class to calculate pagerank
	 */
	public static class PRankReduce extends Reducer<Text, Text, Text, Text> {
		
		/*
		 * 
		 * Reduce function
		 */
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			float sumRank = 0;
			String outStr = "";
			
			// Get the damping factor from the configuration( which is already set in job function)
			// 	of pagerank ie,.PageRankJob()
			Configuration conf = context.getConfiguration();
			String dampFac = conf.get("dampFactor");	
			Float df = Float.parseFloat(dampFac);
			
			float oldRank=0;
			float curRank=0;
			for (Text value : values) {
				if (value.toString().endsWith("$")) {
					StringTokenizer st = new StringTokenizer(value.toString());
					curRank = Float.parseFloat(st.nextToken("$"));
					sumRank += curRank;
					
				} else {
					
					String rankAndEdges =value.toString();
					String[] divideRankAndEdges = rankAndEdges.split(" ",2);
					oldRank= Float.parseFloat(divideRankAndEdges[0]);
						outStr=divideRankAndEdges[1];
				}
				
			}
			
			// include the damping factor
			float newRank = sumRank*df+(1-df);
			
			if ((Math.abs(newRank - oldRank) > 0.001)) {
				context.getCounter(JOB_COUNTER.outOfIterations).increment(1);
			}
			context.write(key, new Text(Float.toString(newRank)+" "+outStr));

		}
	}

	/*
	 * Pagerank job
	 */
	public int PageRankJob(String input, String output,String dampFactor) throws IOException,
			InterruptedException, ClassNotFoundException {
		
		//Create a new configuration
		Configuration configur = new Configuration();
		//create a new job
		Job prJob = new Job(configur, "pagerank");

		//Set eh key,value classes for map output
		prJob.setMapOutputKeyClass(Text.class);
		prJob.setMapOutputValueClass(Text.class);

		//set the mapper class and reducer class
		prJob.setMapperClass(PRankMap.class);
		prJob.setReducerClass(PRankReduce.class);
		
		//push damping  factor to configuration 
		prJob.getConfiguration().set("dampFactor", dampFactor);

		prJob.setInputFormatClass(TextInputFormat.class);
		prJob.setOutputFormatClass(TextOutputFormat.class);
		prJob.setJarByClass(PageRank.class);

		FileInputFormat.addInputPath(prJob, new Path(input));
		FileOutputFormat.setOutputPath(prJob, new Path(output));
		
		prJob.waitForCompletion(true);
		Counters counters = prJob.getCounters();
		Counter countIterations = counters.findCounter(JOB_COUNTER.outOfIterations);		
		return (int) countIterations.getValue();
	}

}
