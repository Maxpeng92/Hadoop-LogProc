package hadoop.logproc.logic;

import hadoop.logproc.data.TextPair;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

public class DirectedJoin extends Configured implements Tool{
	
	private int numReducers;
	private String inputFile;
	private String outputDir;
	private int currentPart;
	private String localHost = "file:///Users/nncsang/Documents/workspace/Hadoop/LogProc/";
	
	public DirectedJoin(String[] args) {
		if (args.length != 3) {
		      System.out.print(args.length);
		      System.out.println("Usage: DirectedJoin <num_reducers> <input_path> <output_path>");
		      System.exit(0);
		    }
		    
		    this.numReducers = Integer.parseInt(args[0]);
		    this.inputFile = args[1];
		    this.outputDir = args[2];
	}
	
	public void deleteFile(Configuration conf, String path){
		try{
			FileSystem fs = FileSystem.get(conf);
		    if(fs.exists(new Path(path))){
		       fs.delete(new Path(path), true);
		    }
		}catch(Exception ex){
			System.out.println(ex.getMessage());
		}
	}
	
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
	    
	    /**	If file output is existed, delete it
	     * 	Remove these line of codes from a real MapReduce application
	     */
	    FileSystem fs = FileSystem.get(conf);
	    if(fs.exists(new Path(outputDir))){
	       fs.delete(new Path(outputDir), true);
	    }
	    
	    //TODO: read input file to parts
	    List<String> parts = new ArrayList<String>();
	    
	    BufferedReader br = new BufferedReader(new FileReader(inputFile));
	    String line;
	    while ((line = br.readLine()) != null) {
	       parts.add(line);
	    }
	    br.close();
	    
	    for(int i = 0; i < parts.size(); i++){
		    // Define new job
	    	conf = this.getConf();
	    	conf.set("partID", parts.get(i));
	    	//conf.set("fs.defaultFS", "hdfs://localhost:9000/");
	    	conf.addResource(new Path("/usr/local/Cellar/hadoop/2.5.0/libexec/etc/hadoop/core-site.xml"));
	    	conf.addResource(new Path("/usr/local/Cellar/hadoop/2.5.0/libexec/etc/hadoop/hdfs-site.xml"));
//	    	conf.addResource(new Path("/usr/local/Cellar/hadoop/2.5.0/libexec/etc/hadoop/mapred-site.xml"));
	    	
		    Job job = new Job(conf, "DirectedJoin");
		    
		    // Set map class and the map output key and value classes
		    
		    job.setMapperClass(DirectedJoinMapper.class);
		    job.setMapOutputKeyClass(NullWritable.class);
		    job.setMapOutputValueClass(Text.class);
		    
		    // Set job output format
		    job.setOutputFormatClass(TextOutputFormat.class);
		    
		    // Set the number of reducers using variable numberReducers
		    job.setNumReduceTasks(0);
		    
		    // Add the input files
		    FileInputFormat.addInputPath(job, new Path(localHost + parts.get(i)));
		    
		    // Set the output path
		    deleteFile(conf, localHost + outputDir + "/" + parts.get(i).substring(1));
		    FileOutputFormat.setOutputPath(job, new Path(localHost + outputDir + "/" + parts.get(i).substring(1)));
		    
		    // Set the jar class
		    job.setJarByClass(DirectedJoin.class);
		    if (job.waitForCompletion(true) == false)
		    	return 0;
	    }	
	    
	    return 1; // this will execute the job
	}
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new DirectedJoin(args), args);
	    System.exit(res);
	}
}

class DirectedJoinMapper extends Mapper<LongWritable, 
							Text, 
							NullWritable, 
							Text> {
	Map<String, String> refTable = new HashMap<String, String>();
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		
		String refTablePath = "r" + context.getConfiguration().get("partID").substring(1);
		Configuration configuration = context.getConfiguration();
		LocalFileSystem localFs = FileSystem.getLocal(configuration);
		
		if(localFs.exists(new Path(refTablePath))) {
			//HRi ← build a hash table from Ri 
			initRefTable(context, refTablePath);
		}else{
			/**
			 * If Ri not exist in local storage then
			 * remotely retrieve Ri and store locally
			 */
			FileSystem hdfsFileSystem = FileSystem.get(context.getConfiguration());
			Path input = new Path("/input1/" + refTablePath);
			Path local = new Path("/Users/nncsang/Documents/workspace/Hadoop/LogProc/");
			
			if (hdfsFileSystem.exists(input)){
				hdfsFileSystem.copyToLocalFile(input, local);
				initRefTable(context, refTablePath);
			}
		}
		
		super.setup(context);
	}
	
	public void initRefTable(Context context, String refTablePath){
		try{
			LocalFileSystem localFileSystem = FileSystem.getLocal(context.getConfiguration());
			BufferedReader br = new BufferedReader(new InputStreamReader(localFileSystem.open(new Path(refTablePath))));
		    String line;
		    while ((line = br.readLine()) != null) {
		       String[] keys = line.split("\t");
		       refTable.put(keys[0], keys[1]);
		    }
		    br.close();
		}catch(Exception ex){
			System.out.println(ex.getMessage());
		}
	}
	
	@Override
	protected void map(LongWritable key, 
						Text value, 
						Context context) throws IOException, InterruptedException {
		/**
		 * Map (K: null, V : a record from a split of Li)
		 * probe HRi with the join column extracted from V for each match r from HRi do
		 * ￼￼￼￼￼￼￼￼emit (null, new record(r, V ))
		 */
		String[] values = value.toString().split("\t");
		String output = values[1] + "\t" + values[2] + "\t" + refTable.get(values[0]);
		context.write(NullWritable.get(), new Text(output));
	}
}


