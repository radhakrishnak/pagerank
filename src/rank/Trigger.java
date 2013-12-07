package rank;
/*
 * Author @radha_krishna
 * Trigger.java
 */

/*
 * Main function in this class. Flow starts here
 */
public class Trigger {
	public static void main(String[] args) throws Exception {
		   
		String input = args[0];
		String output = args[1];
		String dampFactor = args[2];

		 long startTime = System.currentTimeMillis();
	    /*
	     * Graph Properties job
	     */
		 Graph g = new Graph();
	    g.GraphPropertiesJob(input,output+"Graph");
		long endTime = System.currentTimeMillis();
		long timeTakenGraph = endTime-startTime;
		/*
		 * Call InitRank job
		 */
		InitRanks ir = new InitRanks();
		ir.InitRankJob(input,output+"1");

		/*
		  * Call PageRank Job
		  */
		 PageRank pr = new PageRank();
		 startTime = System.currentTimeMillis();
		 
		 int i;
		 for( i=2;i<=60;i++){
			 int counter = pr.PageRankJob(output+(i-1)+"/",output+(i)+"/",dampFactor);
			 if(counter==0){
				 break;
			 }
		 }
		 
		 endTime = System.currentTimeMillis();
		 long timeTakenPR = endTime-startTime;

		 /*
		  * List the Top 10 nodes
		  */
		 Top10 t10 = new Top10();
		 startTime = System.currentTimeMillis();
		 t10.getTop10Nodes(output+i+"/", output+"top10");
		 endTime = System.currentTimeMillis();
		 long timeTakenTop10 = endTime-startTime;
		 
		 /*
		  * print the times 
		  */
		 System.out.println("Time for Graph properties "+timeTakenGraph +" milliSeconds");
		 System.out.println("Time for Calculating Pagerank properties "+timeTakenPR+" milliSeconds");
		 System.out.println("Time taken for getting top 10 nodes "+timeTakenTop10+" milliSeconds");
		 
	 }
}
