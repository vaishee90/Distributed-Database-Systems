package dds.assignment4.EquiJoin;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import java.io.IOException;

// Main class
public class equijoin{
	
	// hashmap to hold the table names and the count to check if at least one value is present for a table
	public static HashMap<String, Integer> tableNames = new HashMap<String, Integer>();
	
	// Mapper class
	public static class EquiJoinMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		// mapper method to set key value pairs
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// obtain join key from the given value
			Text joinKey = new Text((value.toString().split(","))[1].trim());
			
			// obtain table name
			String tableName = (value.toString().split(","))[0].toString();
			
			// put table name and count into hashmap
			if(!tableNames.containsKey(tableName)) {
				tableNames.put(tableName, 0);
			}
			
			// generate output key-value pair
			context.write(joinKey, value);
		}
	}
	
	// Reducer class
	public static class EquiJoinReducer extends Reducer<Text, Text, NullWritable, Text>{

		// reducer method to perform equijoin on the values
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {  
			
			// treemap to store tablename and corresponding values
			TreeMap<String, String> recordList = new TreeMap<String, String>();
			String finRecord = "";
			Boolean incompleteJoin = false;

			// updating hashmap with the count of the table to check if at least one value is present for a table
			for(Text value : values) {
				String[] record = value.toString().split(",");
				tableNames.put(record[0], tableNames.get(record[0]) + 1);
				// updating the table name as key and the corresponding value as value in the treemap
				recordList.put(record[0], value.toString());
			}
			
			// checks for zeroes in the hashmap. If any table has a zero count, then the 
			// current key and values are not written to the output
			for(Integer val : tableNames.values()) {
				if(val < 1)
					incompleteJoin = true;
			}
			
			// if all the tables have a value, then the values in the treemap which were 
			// mapped to a particular key are concatenated (equijoin is performed)
			if(!incompleteJoin) {	
				for(String rec : recordList.values()) {
					if(rec.equals(recordList.get(recordList.firstKey())))
						finRecord = rec;
					else {
						finRecord = finRecord + ", " + rec;
					}
				}
				// generate output key-value pair where key is a Nullwritable value as we are not 
				// writing any key value to the output and value is the output of equijoin between values passed
				context.write(NullWritable.get(), new Text(finRecord));
			}
		}
	}
	
	// Driver specifications given in main method
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		Configuration config = new Configuration();
		
		// initialise a job based on the current config
		Job job = Job.getInstance(config);
		job.setJarByClass(equijoin.class);
        
		// set the output class for key and value generated by the mapper class
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	   
	    // set mapper and reducer class
	    job.setMapperClass(EquiJoinMapper.class);
	    job.setReducerClass(EquiJoinReducer.class);
	   
	    // set the output class for key and value output by the reducer class
	    job.setOutputKeyClass(NullWritable.class);
	    job.setOutputValueClass(Text.class);
	    
	    // set the file input and output paths 
	    FileInputFormat.setInputPaths(job,new Path(args[args.length - 2]));	     
		FileOutputFormat.setOutputPath(job,new Path(args[args.length - 1]));
		
		// system exits only when job completes
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}