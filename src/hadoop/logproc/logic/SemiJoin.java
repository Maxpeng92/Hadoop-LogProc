package hadoop.logproc.logic;

import hadoop.logproc.data.TextPair;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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
	private Path outputDir;
	
	public SemiJoin(String[] args) {
	    if (args.length != 4) {
	      System.out.print(args.length);
	      System.out.println("Usage: StandardRepartitionJoin <num_reducers> <input_ref_path> <input_log_path> <output_path>");
	      System.exit(0);
	    }
	    
	    // Store args
	    this.numReducers = Integer.parseInt(args[0]);
	    this.refFile = new Path(args[1]);
	    this.logFile = new Path(args[2]);
	    this.outputDir = new Path(args[3]);
	}
	  
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
	    
	    /**	If file output is existed, delete it
	     * 	Remove these line of codes from a real MapReduce application
	     */
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(outputDir)){
	       fs.delete(outputDir, true);
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
	    FileOutputFormat.setOutputPath(jobPhase1, outputDir);
	    
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
	   
	    return jobPhase1.waitForCompletion(true) ? 0 : 1; // this will execute the job
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
	protected void cleanup(org.apache.hadoop.mapreduce.Reducer.Context context)
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