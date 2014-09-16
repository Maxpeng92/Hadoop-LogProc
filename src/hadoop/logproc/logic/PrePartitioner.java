package hadoop.logproc.logic;

import hadoop.logproc.data.TextPair;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * 
 * This file implements Directed Join from the paper "A Comparison of Join Algorithms for Log Processing in MapReduce"
 * 
 * Author: NGUYEN Ngoc Chau Sang
 */

public class PrePartitioner extends Configured implements Tool{
	
	static {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory()); 
	}
	
	private int numReducers = 2;
	static String hdfsHost = "hdfs://localhost:9000/"; 
	static Path refFile = new Path("refFile.txt");
	static Path logFile = new Path("logFile.txt");
	static Path outputDir = new Path("output");
	private int maxID = 10;
	private Configuration conf;
	private Job job;
	static String inputType = "r";
	static int interval = 5;
	
	public PrePartitioner(String[] args) {
	}
	  
	@Override
	public int run(String[] arg0) throws Exception {
		conf = this.getConf();
	    
	    /**	If file output is existed, delete it
	     * 	Remove these line of codes from a real MapReduce application
	     */
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(outputDir)){
	       fs.delete(outputDir, true);
	    }
	    
	    // Define new job
	    job = new Job(conf, "PrePartitioner");
	    
	    // Set map class and the map output key and value classes
	    job.setMapperClass(PartitionerMapper.class);
	    job.setMapOutputKeyClass(IntWritable.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    // Set job output format
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    job.setReducerClass(PartitionerReducer.class);
	    job.setOutputKeyClass(IntWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    // Set the number of reducers using variable numberReducers
	    job.setNumReduceTasks(maxID/interval + 1);
	    job.setPartitionerClass(DirectedPartitioner.class);
	    
	    // Add the input files
	    FileInputFormat.addInputPath(job, refFile);
	    
	    // Set the output path
	    FileOutputFormat.setOutputPath(job, outputDir);
	    
	    // Set the jar class
	    job.setJarByClass(PrePartitioner.class);
	    
//	    
//	    InputStream in = null;
//	    try {
//	    	in = new URL(PrePartitioner.hdfsHost + "input/exclude.txt").openStream();
//	    	IOUtils.copyBytes(in, System.out, 4096, false); 
//	    } finally {
//	    	IOUtils.closeStream(in); 
//	    }
	    
//	    in = new BufferedInputStream(new FileInputStream("logFile.txt"));
//	    
//	    fs = FileSystem.get(URI.create(DirectedJoin.hdfsHost + "input/logFile.txt"), conf); 
//	    OutputStream out = fs.create(new Path(DirectedJoin.hdfsHost + "input/logFile.txt"), new Progressable() {
//	    	public void progress() {
//	    	System.out.print("."); }
//	    	});
//	    IOUtils.copyBytes(in, out, 4096, true);
	    	
	    return 0;//job.waitForCompletion(true) ? 0 : 1; // this will execute the job
	}
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new PrePartitioner(args), args);
	    System.exit(res);
	}
}

class DirectedPartitioner extends Partitioner<IntWritable, Text>{

	@Override
	public int getPartition(IntWritable key, Text value, int numPartitions) {
		return key.get() / PrePartitioner.interval;
	}
	
}

class PartitionerReducer extends Reducer<IntWritable, 
										Text, 
										Text, 
										Text> { 

	@Override
	protected void reduce(IntWritable key, 
			Iterable<Text> values, 
			Context context) throws IOException, InterruptedException {
		int interval = 5;
		int partition = key.get() / interval;
		
		final Iterable<Text> vls = values;
		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataOutputStream out = fs.create(new Path (PrePartitioner.outputDir + "/" +  PrePartitioner.inputType + "/"+ partition + ".txt"), new Progressable() {
			@Override
			public void progress() {
				Iterator iter = vls.iterator();
				while(iter.hasNext()){
					System.out.println(iter.next().toString());
				}
			}
		});
	}
}


class PartitionerMapper extends Mapper<LongWritable, 
							Text, 
							IntWritable, 
							Text> { 
	@Override
	protected void map(LongWritable key, 
						Text value, 
						Context context) throws IOException, InterruptedException {
		
		String[] values = value.toString().split("\t");
		context.write(new IntWritable(Integer.parseInt(values[0])) , value);
	}
}


