package hadoop.logproc.logic;

import hadoop.logproc.data.TextPair;

import java.io.IOException;
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

public class StandardRepartitionJoin extends Configured implements Tool{
	private int numReducers;
	private Path refFile;
	private Path logFile;
	private Path outputDir;
	
	public StandardRepartitionJoin(String[] args) {
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
	    
	    // Define new job
	    Job job = new Job(conf, "StandardRepartitionJoin"); //define new job
	    
	    
	    // Set job output format
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    // Add the input files 
	    MultipleInputs.addInputPath(job, refFile, TextInputFormat.class, StandardRepartitionJoinRefMapper.class);
	    MultipleInputs.addInputPath(job, logFile, TextInputFormat.class, StandardRepartitionJoinLogMapper.class);
	    
	    
	    // Set the output path
	    FileOutputFormat.setOutputPath(job, outputDir);
	    
	    // Set reduce class and the reduce output key and value classes
	    job.setReducerClass(StandardRepartitionJoinReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    // Set map class and the map output key and value classes
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(TextPair.class);
	    
	    // Set the number of reducers using variable numberReducers
	    job.setNumReduceTasks(numReducers);
	    
	    // Set the jar class
	    job.setJarByClass(StandardRepartitionJoin.class);
	   
	    return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
	}
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new StandardRepartitionJoin(args), args);
	    System.exit(res);
	}
}

class StandardRepartitionJoinRefMapper extends Mapper<LongWritable, 
								Text, 
								Text, 
								TextPair> { 
	@Override
	protected void map(LongWritable key, 
						Text value, 
						Context context) throws IOException, InterruptedException {
		
		String[] values = value.toString().split("\t");
		/**
		 * Extract the join column from V
		 * Add a tag of either R or L to V
		 * Here 0: mean R
		 */
		context.write(new Text(values[0]), new TextPair(new Text("0"), new Text(values[1])));
	}
}

class StandardRepartitionJoinLogMapper extends Mapper<LongWritable, 
														Text, 
														Text, 
														TextPair> { 

	@Override
	protected void map(LongWritable key, 
						Text value, 
						Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\t");
		/**
		 * Extract the join column from V
		 * Add a tag of either R or L to V
		 * Here 1: mean L
		 */
		context.write(new Text(values[0]), new TextPair(new Text("1"), new Text(values[1] + " - " + values[2])));
	}
}

class StandardRepartitionJoinReducer extends Reducer<Text, 
											TextPair, 
  											Text, 
  											Text> { 

	@Override
	protected void reduce(Text key, 
							Iterable<TextPair> values, 
							Context context) throws IOException, InterruptedException {
		Iterator<TextPair> iter = values.iterator();
		
		// Create buffers BR and BL for R and L, respectively
		List<String> ref = new ArrayList<String>();
		List<String> log = new ArrayList<String>();
		
		/**	For each record t in LIST V′ do
		 *	append t to one of the buffers according to its tag
		 */
		TextPair value;
		while (iter.hasNext()){
			value = iter.next();
			if (Integer.parseInt(value.getFirst().toString()) == 0){
				ref.add(value.getSecond().toString());
			}else{
				log.add(value.getSecond().toString());
			}
		}
		
		/**
		 * For each pair of records (r, l) in BR × BL do cross product
		 */
		for(int i = 0; i < ref.size(); i++)
			for(int j = 0; j < log.size(); j++)
				context.write(new Text(log.get(j)), new Text(ref.get(i)));
	}
}



