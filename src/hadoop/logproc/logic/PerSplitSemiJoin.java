package hadoop.logproc.logic;

import hadoop.logproc.data.TextPair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 
 * This file implements PerSplitSemiJoin from the paper "A Comparison of Join Algorithms for Log Processing in MapReduce"
 * 
 * Author: NGUYEN Ngoc Chau Sang
 */

public class PerSplitSemiJoin extends Configured implements Tool{
	private int numReducers;
	private Path refFile;
	private Path logFile;
	private Path outputDirPhase1;
	private Path outputDirPhase2;
	
	public PerSplitSemiJoin(String[] args) {
	    if (args.length != 5) {
	      System.out.print(args.length);
	      System.out.println("Usage: StandardRepartitionJoin <num_reducers> <input_ref_path> <input_log_path> <output_phase_1_path> <output_phase_2_path>");
	      System.exit(0);
	    }
	    
	    // Store args
	    this.numReducers = Integer.parseInt(args[0]);
	    this.refFile = new Path(args[1]);
	    this.logFile = new Path(args[2]);
	    this.outputDirPhase1 = new Path(args[3]);
	    this.outputDirPhase2 = new Path(args[4]);
	}
	  
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
	    
	    /**	If file output is existed, delete it
	     * 	Remove these line of codes from a real MapReduce application
	     */
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(outputDirPhase1)){
	       fs.delete(outputDirPhase1, true);
	    }
	    
	    /**	Define new job
	     * 	Phase 1: Extract unique join keys for each L split to Li.uk
	     */
	    Job jobPhase1 = new Job(conf, "PerSliptSemiJoinPhase1"); //define new job
	    
	    // Set job output format
	    jobPhase1.setOutputFormatClass(TextOutputFormat.class);
	    
	    // Add the input files 
	    FileInputFormat.addInputPath(jobPhase1, logFile);
	    // Set the output path
	    FileOutputFormat.setOutputPath(jobPhase1, outputDirPhase1);
	
	    
	    // Set map class and the map output key and value classes
	    jobPhase1.setMapperClass(PerSplitSemiJoinPhase1Mapper.class);
	    jobPhase1.setMapOutputKeyClass(Text.class);
	    jobPhase1.setMapOutputValueClass(NullWritable.class);
	    
	    // Set the number of reducers using variable numberReducers
	    jobPhase1.setNumReduceTasks(0);
	    
	    // Set the jar class
	    jobPhase1.setJarByClass(PerSplitSemiJoin.class);
	   
	    if (jobPhase1.waitForCompletion(true) == false){
	    	return 0;
	    }
	    
	    /**	Define new job
	     * 	Phase 2: Use Li.uk to filter referenced R; generate a file RLi for each Li
	     */
	    
	    // Set job output format
	    File folder = new File(outputDirPhase1.toString());
	    File[] listOfFiles = folder.listFiles();

	    for (File file : listOfFiles) {
	        if (file.isFile()) {
	            String fileName = file.getName();
	            if (fileName == "_SUCCESS" || fileName.charAt(0) == '.' || fileName.charAt(0) == '_')
	            	continue;
	            Job jobPhase2 = new Job(conf, "PerSliptSemiJoinPhase2"); //define new job
	            jobPhase2.setOutputFormatClass(TextOutputFormat.class);
	            
	            // Add the input files 
	    	    FileInputFormat.addInputPath(jobPhase2, refFile);
	    	    
	    	    if(fs.exists(new Path(outputDirPhase2.toString() + "//" + fileName))){
	 		       fs.delete(new Path(outputDirPhase2.toString() + "//" + fileName), true);
	 		    }
	    	    // Set the output path
	    	    FileOutputFormat.setOutputPath(jobPhase2, new Path(outputDirPhase2.toString() + "//" + fileName));
	    	    
	    	    // Set map class and the map output key and value classes
	    	    jobPhase2.setMapperClass(PerSplitSemiJoinPhase2Mapper.class);
	    	    jobPhase2.setMapOutputKeyClass(NullWritable.class);
	    	    jobPhase2.setMapOutputValueClass(Text.class);
	    	    
	    	    /**
	    	     *  Init ()
	    	     *  ref keys ← load L.uk from phase 1 to a hash tables
	    	     */
	    	    PerSplitSemiJoinPhase2Mapper.init(outputDirPhase1 + "//" +fileName);
	    	    // Set the number of reducers using variable numberReducers
	    	    jobPhase2.setNumReduceTasks(numReducers);
	    	    
	    	    // Set the jar class
	    	    jobPhase2.setJarByClass(PerSplitSemiJoin.class);
	    	    if (jobPhase2.waitForCompletion(true) == false)
	    	    	return 0;
	        }
	    }
	   
	    
	    /**
	     * Phase 3: Directed join between each RLi and Li pair
	     * which is implemented in DirectedJoin.java
	     */
	    return 0; // this will execute the job
	}
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new PerSplitSemiJoin(args), args);
	    System.exit(res);
	}
}

class PerSplitSemiJoinPhase1Mapper extends Mapper<LongWritable, 
								Text, 
								Text, 
								NullWritable> {
	Set<String> hashTable = new HashSet<String>();
	@Override
	protected void map(LongWritable key, 
						Text value, 
						Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\t");
		if (hashTable.contains(values[0]) == false){
			hashTable.add(values[0]);
			context.write(new Text(values[0]), NullWritable.get());
		}
	}
}

class PerSplitSemiJoinPhase2Mapper extends Mapper<LongWritable, 
											Text, 
											NullWritable, 
											Text> {
	static Set<String> hashTable = new HashSet<String>();
	static public void init(String uniqueKeysPath){
		hashTable.clear();
		try{
			BufferedReader br = new BufferedReader(new FileReader(uniqueKeysPath));
		    String line;
		    while ((line = br.readLine()) != null) {
		    	hashTable.add(line);
		    }
	    }catch(Exception ex){
	    	System.out.println(ex.getMessage());
	    }
	}
	
	@Override
	protected void map(LongWritable key, 
						Text value, 
						Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\t");
		/**
		 * join col ← extract join column from V if join col in ref keys then
		 * emit (null, V )
		 */
		if (hashTable.contains(values[0]))
			context.write(NullWritable.get(), value);
	}
}