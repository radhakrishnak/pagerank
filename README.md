pagerank
========

This is implementation of PageRank algorithm using Java

Trigger.java
  The Main() function of the program is located in this file. The execution starts from here. It collects the command line arguments and calls the subsequent jobs from the main function. Initially Graph properties job is called first, InitRanks second, PageRank third and Top10 ranks fourth.

Graph.java
  This class creates a job, Mapper & Reducer for calculating graph properties like Minimum out-degree, Maximum out- degree, Average out-degree, Number of nodes and Number of edges. It creates the job in GraphPropertiesJob() function.

InitRanks.java
  This class contains a job, Mapper & Reducer for initializing the page ranks to all nodes. Call to mapper is made from the job function, InitRankJob().

PageRank.java
  This class contains a job, Mapper & Reducer for calculating the Page Ranks for all the nodes. It also considers the Damping factor while calculating the rank for next iteration.

Top10.java
  This class contains a job, Mapper & Reducer implementations for getting the Top 10 nodes in the result from Page Rank job. The ranks are ordered in Descending order.
  

steps to run
============

The pagerank.jar file takes 3 command line arguments They are:
1. input directory name
2. output directory starting name (this name will be used to create the directories for all jobs, eg,. If argument is “output”
then output for graph properties will be created as “outputGraph”, for Initialize ranks it will be created as “output1” and the remaining “output<number>” will be created for calculating page ranks till they converge and for Top 10 will be created as “outputTop10”.
3. Damping Factor
