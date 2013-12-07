package rank;
/*
 * Author @radha_krishna
 * InitRanks.java
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



public class InitRanks {

	/*
	 * Map class for Initialization of graph
	 */
	public static class PGMap extends Mapper<LongWritable, Text, Text, Text> {

		private Text nodeID = new Text();
		private Text rankNodes = new Text();

		/*
		 *Map function
		 * 
		 */
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer st = new StringTokenizer(line);

			String node = st.nextToken();
			String initialRank = "1.0 ";

			while (st.hasMoreTokens()) {
				initialRank += st.nextToken();
				initialRank += " ";
			}

			nodeID.set(node);
			rankNodes.set(initialRank);

			context.write(nodeID, rankNodes);
		}
		
	}
	
	/*
	 * Reducer class for Initialization
	 */
	public static class PGReduce extends Reducer<Text, Text, Text, Text> {

		/*
		 * Reduce class 
		 */
		public void reduce(Text key, Text value, Context context) 
				throws IOException, InterruptedException {
			
			context.write(key, value);
		}
	}
	
	/*
	 * Job for Graph Initialization
	 */
	
	public void InitRankJob(String input, String outputInit) throws IOException, InterruptedException, ClassNotFoundException{

		// create configuration 
		Configuration conf = new Configuration();
		//create new job for initializing the graph
		Job prJob = new Job(conf, "initrank");
		
		//set the key,value classes for output
		 prJob.setOutputKeyClass(Text.class);
		 prJob.setOutputValueClass(Text.class);

		 //set the mapper and reducer class types
		 prJob.setMapperClass(PGMap.class);
		 prJob.setReducerClass(PGReduce.class);

		 prJob.setInputFormatClass(TextInputFormat.class);
		 prJob.setOutputFormatClass(TextOutputFormat.class);
		 
		 prJob.setJarByClass(InitRanks.class);

		 FileInputFormat.addInputPath(prJob, new Path(input));
		 FileOutputFormat.setOutputPath(prJob, new Path(outputInit));

		 //wait for job to complete
		 prJob.waitForCompletion(true);
		 
	 }
}
