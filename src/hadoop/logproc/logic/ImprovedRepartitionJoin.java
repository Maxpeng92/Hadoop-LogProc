package hadoop.logproc.logic;

import hadoop.logproc.data.TextPair;

import java.io.IOException;
import java.util.Iterator;
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
 * 
 * 
 * 
 */
public class ImprovedRepartitionJoin extends Configured implements Tool{
	private int numReducers;
	private Path refFile;
	private Path logFile;
	private Path outputDir;
	
	public ImprovedRepartitionJoin(String[] args) {
	    if (args.length != 4) {
	      System.out.print(args.length);
	      System.out.println("Usage: WordCount <num_reducers> <input_ref_path> <input_log_path> <output_path>");
	      System.exit(0);
	    }
	    
	    this.numReducers = Integer.parseInt(args[0]);
	    this.refFile = new Path(args[1]);
	    this.logFile = new Path(args[2]);
	    this.outputDir = new Path(args[3]);
	}
	  
	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = this.getConf();
	    
	    /**	If file output is existed, delete it
	     * 	Delete it from a real MapReduce application
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
	    MultipleInputs.addInputPath(job, refFile, TextInputFormat.class, ImprovedRepartitionJoinRefMapper.class);
	    MultipleInputs.addInputPath(job, logFile, TextInputFormat.class, ImprovedRepartitionJoinLogMapper.class);
	    
	    
	    // Set the output path
	    FileOutputFormat.setOutputPath(job, outputDir);
	    
	    // Set reduce class and the reduce output key and value classes
	    job.setReducerClass(ImprovedRepartitionJoinReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    //job.setSortComparatorClass(ValueComparator.class);
	    
	    // Set map class and the map output key and value classes
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(TextPair.class);
	    
	    // Set the number of reducers using variable numberReducers
	    job.setNumReduceTasks(numReducers);
	    
	    // Set the jar class
	    job.setJarByClass(ImprovedRepartitionJoin.class);
	   
	    return job.waitForCompletion(true) ? 0 : 1; // this will execute the job
	}
	
	public static void main(String args[]) throws Exception {
	    int res = ToolRunner.run(new Configuration(), new ImprovedRepartitionJoin(args), args);
	    System.exit(res);
	}
	
	static class ValueComparator extends WritableComparator implements RawComparator {

		protected ValueComparator(Class<TextPair> keyClass) {
			super(TextPair.class);
			// TODO Auto-generated constructor stub
		} 
		
		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			int type1 = Integer.parseInt(((TextPair) a).getFirst().toString());
			int type2 = Integer.parseInt(((TextPair) b).getFirst().toString());
			
			if (type1 > type2)
				return 1;
			if (type1 == type2)
				return 0;
			return -1;
		}
	}
}

class ImprovedRepartitionJoinRefMapper extends Mapper<LongWritable, 
								Text, 
								Text, 
								TextPair> { 
	@Override
	protected void map(LongWritable key, 
						Text value, 
						Context context) throws IOException, InterruptedException {
		
		String[] values = value.toString().split("\t");
		context.write(new Text(values[0]), new TextPair(new Text("0"), new Text(values[1])));
	}
}

class ImprovedRepartitionJoinLogMapper extends Mapper<LongWritable, 
														Text, 
														Text, 
														TextPair> { 

	@Override
	protected void map(LongWritable key, 
						Text value, 
						Context context) throws IOException, InterruptedException {
		String[] values = value.toString().split("\t");
		context.write(new Text(values[0]), new TextPair(new Text("1"), new Text(values[1] + " - " + values[2])));
	}
}

class ImprovedRepartitionJoinReducer extends Reducer<Text, 
											TextPair, 
  											Text, 
  											Text> { 

	@Override
	protected void reduce(Text key, 
							Iterable<TextPair> values, 
							Context context) throws IOException, InterruptedException {
		Iterator<TextPair> iter = values.iterator();
		try{
			
			TextPair firstRecord = iter.next();
			String type  = firstRecord.getFirst().toString();
			String value  = firstRecord.getSecond().toString();
			
			while(iter.hasNext()){
				context.write(iter.next().getSecond(), new Text(value));
			}
			
		}catch(Exception ex){
			System.out.println(ex.getMessage());
		}
  
	}
}


