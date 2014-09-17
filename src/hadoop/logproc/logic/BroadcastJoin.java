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
import java.io.PrintWriter;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
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
 * This file implements Broadcast Join from the paper "A Comparison of Join Algorithms for Log Processing in MapReduce"
 * 
 * Author: NGUYEN Ngoc Chau Sang
 */

public class BroadcastJoin extends Configured implements Tool{
	
	static String localHost = "file:///Users/nncsang/Documents/workspace/Hadoop/LogProc/";
	static String inputType = "r";
	static int interval = 5;
	static int maxSizeInputSplit = 64; // unit: MB, change the value depend your system
	static int maxID = 10;
	
	private String refFile;
	private String logFile;
	private String outputDir;
	private String refFileName = "refFile.txt";
	private int currentPart;
	private int numReducers;
	
	
	public BroadcastJoin(String[] args) {
		if (args.length != 3) {
		      System.out.print(args.length);
		      System.out.println("Usage: DirectedJoin <refFile_path> <logFile_path> <output_path>");
		      System.exit(0);
		    }
		    
		    this.refFile = args[0];
		    refFileName = refFile;
		    this.logFile = args[1];
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
	    
		// Define new job
	    conf = this.getConf();
	    conf.addResource(new Path("/usr/local/Cellar/hadoop/2.5.0/libexec/etc/hadoop/core-site.xml"));
	    conf.addResource(new Path("/usr/local/Cellar/hadoop/2.5.0/libexec/etc/hadoop/hdfs-site.xml"));
	    conf.set("refFileName", refFileName);
	    
	    /*
	     * Init ()
	     */
	    BroadcastJoinMapper.init(conf);
	    
	    // //TODO: read input file to parts
	    List<String> parts = new ArrayList<String>();
	    
	    BufferedReader br = new BufferedReader(new FileReader(logFile));
	    String line;
	    while ((line = br.readLine()) != null) {
	       parts.add(line);
	    }
	    br.close();
	    
	    for(int i = 0; i < parts.size(); i++){
		    // Define new job
	    	conf = this.getConf();
	    	conf.set("partID", parts.get(i).substring(0, parts.get(i).length() - 4));
			Job job = new Job(conf, "BroadcastJoin");
			    
			// Set map class and the map output key and value classes
			    
			job.setMapperClass(BroadcastJoinMapper.class);
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
			job.setJarByClass(BroadcastJoin.class);
	    
			  if (job.waitForCompletion(true) == false)
			    	return 0;
	    }	
		    
	    return 1; // this will execute the job
	}
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new BroadcastJoin(args), args);
	    System.exit(res);
	}
}

class BroadcastJoinMapper extends Mapper<LongWritable, 
							Text, 
							NullWritable, 
							Text> {
	
	static Map<String, String> refTable = new HashMap<String, String>();
	static List<Map<String, String>> refTables = new ArrayList<Map<String, String>>();
	static List<ArrayList<String>> valueTables = new ArrayList<ArrayList<String>>();
	static int mode = 0;
	int partID = -1;
	
	public static void init(Configuration conf)
			throws IOException, InterruptedException {

		String refTablePath = conf.get("refFileName");
		Configuration configuration = conf;
		LocalFileSystem localFs = FileSystem.getLocal(configuration);
		int numOfPartitions = 0;
		/**
		 * if R not exist in local storage then
		 */
		if(!localFs.exists(new Path(refTablePath))) {
			/*
			 * remotely retrieve R
			 * partition R into p chunks R1..Rp save R1..Rp to local storage
			 */
			numOfPartitions = initRefFiles(conf, refTablePath);
		}
		
		File file = new File(refTablePath);
		long size = file.length() / (1024 * 1024);
		
		/**
		 * if R < a split of L then
		 */
		if (BroadcastJoin.maxSizeInputSplit > size ){
			//HR ← build a hash table from R1..Rp
			mode = 0;
			initRefTable(conf, refTablePath);
		}else{
			//HL1 ..HLp ← initialize p hash tables for L
			mode = 1;
			initRefTables(conf, refTablePath, BroadcastJoin.maxID / BroadcastJoin.interval);
		}
	}
	
	public static void initRefTable(Configuration conf, String refTablePath){
		try{
			LocalFileSystem localFileSystem = FileSystem.getLocal(conf);
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
	
	public static void initRefTables(Configuration conf, String refTablePath, int numOfPartitions){
		try{
			for(int i = 0; i < numOfPartitions; i++){
				refTables.add(new HashMap<String, String>());
				valueTables.add(new ArrayList<String>());
			}
		}catch(Exception ex){
			System.out.println(ex.getMessage());
		}
	}
	
	public static int initRefFiles(Configuration conf, String refTablePath){
		try{
			FileSystem hdfsFileSystem = FileSystem.get(conf);
			Path input = new Path("/input1/" + refTablePath);
			Path local = new Path(BroadcastJoin.localHost);
				
			if (hdfsFileSystem.exists(input)){
				/**
				 * remotely retrieve R
				 */
				hdfsFileSystem.copyToLocalFile(input, local);
				
				/**
				 * Partition R into p chunks R1..Rp
				 * This is not a good implementation, better way: using MapReduce for partitioning R (PrePartitioner.java)
				 */
				Map<Integer, ArrayList<String>> buffer = new HashMap<Integer, ArrayList<String>>(); // <--- very critical memory problem
				
				BufferedReader br = new BufferedReader(new FileReader(refTablePath));
				
				String line;
				while ((line = br.readLine()) != null) {
					String[] words = line.split("\t");
					int index = Integer.parseInt(words[0]) / BroadcastJoin.interval;
					if (!buffer.containsKey(index)){
						buffer.put(index, new ArrayList<String>());
					}
					buffer.get(index).add(line);
				}
				br.close();
				
				/**
				 *  Save R1..Rp to local storage
				 */
				Set<Entry<Integer, ArrayList<String>>> entries = buffer.entrySet();
				for(Entry<Integer, ArrayList<String>> entry : entries){
					String chunkName = "r" + entry.getKey().toString() + ".txt";
					PrintWriter writer = new PrintWriter(chunkName, "UTF-8");
					
					ArrayList<String> values = entry.getValue();
					for(int i = 0; i < values.size(); i++){
						writer.println(values.get(i));
					}
					
					writer.close();
				}
				return buffer.size();
			}
		}catch(Exception ex){
			System.out.println(ex.getMessage());
		}
		
		return 0;
	}
	
	@Override
	protected void map(LongWritable key, Text value, 
						Context context) throws IOException, InterruptedException {
		// if HR exist then
		if (mode == 0){
			/**
			 * probe HR with the join column extracted from V for each match r from HR do
			 * emit (null, new record(r, V ))
			 */
			
			String[] values = value.toString().split("\t");
			String output = values[1] + "\t" + values[2] + "\t" + refTable.get(values[0]);
			context.write(NullWritable.get(), new Text(output));
		}else{
			// add V to an HLi hashing its join column
			String confString = context.getConfiguration().get("partID");
			partID = Integer.parseInt(context.getConfiguration().get("partID").substring(1));
			valueTables.get(partID).add(value.toString());
		}
	}
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		
		// if HR not exist then
		if (mode == 1){ 
			// load Ri in memory
			if (refTables.get(partID).size() == 0){
				LocalFileSystem localFileSystem = FileSystem.getLocal(context.getConfiguration());
				BufferedReader br = new BufferedReader(new InputStreamReader(localFileSystem.open(new Path("r" + partID + ".txt"))));
			    String line;
			    while ((line = br.readLine()) != null) {
			       String[] keys = line.split("\t");
			       refTables.get(partID).put(keys[0], keys[1]);
			    }
			    br.close();
			}
			
		    for(String value: valueTables.get(partID)){
		    	String[] values = value.toString().split("\t");
				String output = values[1] + "\t" + values[2] + "\t" + refTables.get(partID).get(values[0]);
				context.write(NullWritable.get(), new Text(output));
		    }
		}
	}
}


