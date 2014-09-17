package hadoop.logproc.logic;

import hadoop.logproc.data.TextPair;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
 * This file implements Standard Repartition Join from the paper "A Comparison of Join Algorithms for Log Processing in MapReduce"
 * 
 * Author: NGUYEN Ngoc Chau Sang
 */

public class SemiJoin extends Configured implements Tool{
	private int numReducers;
	private Path refFile;
	private Path logFile;
	private Path outputDirPhase1;
	private Path outputDirPhase2;
	
	public SemiJoin(String[] args) {
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
	     * 	Phase 1: Extract unique join keys in L to a single file L.uk
	     */
	    Job jobPhase1 = new Job(conf, "SemiJoinPhase1"); //define new job
	    
	    // Set job output format
	    jobPhase1.setOutputFormatClass(TextOutputFormat.class);
	    
	    // Add the input files 
	    FileInputFormat.addInputPath(jobPhase1, logFile);
	    // Set the output path
	    FileOutputFormat.setOutputPath(jobPhase1, outputDirPhase1);
	    
	    // Set reduce class and the reduce output key and value classes
	    jobPhase1.setReducerClass(SemiJoinPhase1Reducer.class);
	    jobPhase1.setOutputKeyClass(Text.class);
	    jobPhase1.setOutputValueClass(Text.class);
	    
	    // Set map class and the map output key and value classes
	    jobPhase1.setMapperClass(SemiJoinPhase1Mapper.class);
	    jobPhase1.setMapOutputKeyClass(Text.class);
	    jobPhase1.setMapOutputValueClass(NullWritable.class);
	    
	    // Set the number of reducers using variable numberReducers
	    jobPhase1.setNumReduceTasks(numReducers);
	    
	    // Set the jar class
	    jobPhase1.setJarByClass(SemiJoin.class);
	   
	    if (jobPhase1.waitForCompletion(true) == false){
	    	return 0;
	    }
	    /**	Define new job
	     * 	Phase 2: Use L.uk to filter referenced R records; generate a file Ri for each R split
	     */
	    Job jobPhase2 = new Job(conf, "SemiJoinPhase2"); //define new job
	    
	    /**	If file output is existed, delete it
	     * 	Remove these line of codes from a real MapReduce application
	     */
	    if(fs.exists(outputDirPhase2)){
	       fs.delete(outputDirPhase2, true);
	    }
	    
	    // Set job output format
	    jobPhase2.setOutputFormatClass(TextOutputFormat.class);
	    
	    // Add the input files 
	    FileInputFormat.addInputPath(jobPhase2, refFile);
	    
	    // Set the output path
	    FileOutputFormat.setOutputPath(jobPhase2, outputDirPhase2);
	    
	    // Set map class and the map output key and value classes
	    jobPhase2.setMapperClass(SemiJoinPhase2Mapper.class);
	    jobPhase2.setMapOutputKeyClass(NullWritable.class);
	    jobPhase2.setMapOutputValueClass(Text.class);
	    
	    /**
	     *  Init ()
	     *  ref keys ← load L.uk from phase 1 to a hash tables
	     */
	    SemiJoinPhase2Mapper.init("semi_join_phase1_uniquekeys.txt");
	    
	    // Set the number of reducers using variable numberReducers
	    jobPhase2.setNumReduceTasks(numReducers);
	    
	    // Set the jar class
	    jobPhase2.setJarByClass(SemiJoin.class);
	    
	    /**
	     * Phase 3: Broadcast all Ri to each L split for the final join
	     * which is implemented in BroadcaseJoin.java
	     */
	    return jobPhase2.waitForCompletion(true) ? 0 : 1; // this will execute the job
	}
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new SemiJoin(args), args);
	    System.exit(res);
	}
}

class SemiJoinPhase1Mapper extends Mapper<LongWritable, 
								Text, 
								Text, 
								NullWritable> { 
	@Override
	protected void map(LongWritable key, 
						Text value, 
						Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\t");
		context.write(new Text(values[0]), NullWritable.get());
	}
}

class SemiJoinPhase1Reducer extends Reducer<Text, 
											NullWritable, 
											NullWritable, 
											NullWritable> { 
	private int sizePerFlush = 5;
	private List<String> uniqueKeys = new ArrayList<String>();
	@Override
	protected void reduce(Text key, 
							Iterable<NullWritable> values, 
							Context context) throws IOException, InterruptedException {
		uniqueKeys.add(key.toString());
		if (uniqueKeys.size() > sizePerFlush){
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("semi_join_phase1_uniquekeys.txt", true)));
			for(String uniquekey: uniqueKeys){
				out.println(uniquekey);
			}
			out.close();
			uniqueKeys.clear();
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		if (uniqueKeys.size() > 0){
			PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("semi_join_phase1_uniquekeys.txt", true)));
			for(String uniquekey: uniqueKeys){
				out.println(uniquekey);
			}
			out.close();
			uniqueKeys.clear();
		}
	}
}

class SemiJoinPhase2Mapper extends Mapper<LongWritable, 
											Text, 
											NullWritable, 
											Text> {
	static Set<String> hashTable = new HashSet<String>();
	static public void init(String uniqueKeysPath){
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