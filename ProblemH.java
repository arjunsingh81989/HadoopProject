import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class ProblemH extends Configured implements Tool {
    
    // Temporary output folder for joining Users and Posts data
    private static final Path TEMP_OUTPUT = new Path("temp-H");

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new ProblemH(), args);
		System.exit(res);
	}

    /**
     * Configure MapReduce (MR) jobs for a MR program to solve problem H 
     * of the StackOverflow data.
     */
	public int run(String[] args) throws Exception {
	    // Command-line arguments for running this MR program:
        // Path (in HDFS) to a CSV file or directory of CSV files of User data
	    Path usersPath = new Path(args[0]);
        // Path (in HDFS) to store output that contains answer for the problem
		Path outputPath = new Path(args[1]);

		Configuration conf = getConf();

        // Delete temporary output and output folder before running. 
        FileSystem fs = FileSystem.get(conf);
        fs.delete(TEMP_OUTPUT, true);
        fs.delete(outputPath, true);
        
		/*
		 * Job 1:
         * - Read users data to count number of users per location
		 * 
		 * Mapper class: UserCountMapper
		 * Reducer class: UserCountReducer
		 */
		Job job1 = new Job(conf, "ProbH-Job1");
		job1.setJarByClass(ProblemH.class);
        
        // Configure classes of key-value output from a Mapper task
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        // Configure classes of key-value output from a Reducer task
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // Set which class is used to create a Mapper task
        job1.setMapperClass(UserCountMapper.class);
        // Set which class is used to create a Reducer task
		job1.setReducerClass(UserCountReducer.class);
		
		// Set input and output path of Job1
		FileInputFormat.setInputPaths(job1, usersPath);
		FileOutputFormat.setOutputPath(job1, TEMP_OUTPUT); 
		
        // Tell Hadoop to wait for Job1 completed before running Job2
		job1.waitForCompletion(true);
		
		/*
		 * Job 2: From job1's output, the location having the maximum
		 * number of users
		 * 
		 * Mapper class:  MaxCountMapper
		 * Reducer class: MaxCountReducer
		 */

		Job job2 = new Job(conf, "ProbH-Job2");
        job2.setJarByClass(ProblemH.class);

        // Configure classes of key-value output from a Mapper task
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Set which class is used to create a Mapper task
        job2.setMapperClass(MaxCountMapper.class);        
        // Set which class is used to create a Reducer task
        job2.setReducerClass(MaxCountReducer.class);
        
        // Sort by descending order of keys (count values) so that
        // the first record comes to the reducer is the maximum count value
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        // Both input and output are in text format
		job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // Input path for Job2 is the output path of Job1
        FileInputFormat.setInputPaths(job2, TEMP_OUTPUT);
        // Output path for Job2 is the final output
        FileOutputFormat.setOutputPath(job2, outputPath);

        // Tell Hadoop to wait for Job2 completed before cleaning up 
        // the MR program
        return job2.waitForCompletion(true) ? 0 : 1;
	}
	
	/**
     * Mapper to read users data file to count user per location
     * 
     * Output: one Key-Value pair per record
     *     Key: Location
     *     Value: 1
     */
    public static class UserCountMapper 
    extends Mapper<LongWritable, Text, Text, IntWritable> {
        
        private Text outKey = new Text();
        private IntWritable one = new IntWritable(1);
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            
            // Parse an input line for details of a User
            InputParser.User user = InputParser.parseUser(value.toString());
            
            // Make sure location data is valid
            if (user.location.length() > 0) {
                // Output the key is the location, and value is to count that
                // one user has been found in the location
                outKey.set(user.location);
                context.write(outKey, one);
            }
        }
    }
	
	/**
	 * Reducer to count number of users per location
	 *
	 * Output: one Key-Value pair per location
	 *     Key: Location
	 *     Value: UserCount
	 */
	public static class UserCountReducer extends
    Reducer<Text, IntWritable, Text, IntWritable> {
	    
	    private IntWritable outValue = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                Reducer<Text, IntWritable, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            
            // Count now are grouped by key - location name
            // So we can count total number of user per distinct location
            int count = 0;
            for (IntWritable value: values) {
                count++;
            }
            
            outValue.set(count);
            context.write(key, outValue);
        }
	}
	
	/**
	 * Mapper: parse job1's output to find location with local maximum number
	 * of users (among locations proccessed by an individual mapper task)
	 * 
	 * Output: One key-value pair per task
	 *     Key: UserCount
     *     Value: Location
	 */
	public static class MaxCountMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
	    
	    // Save maximum user count of a location (local maximum value) found
	    // in the data split fed to this mapper
	    private int maxUsers = 0;
	    
	    // Also save the location of the max user count
	    private String locationWithMaxUsers = null;
	    
	    private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();
	    
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Input is the output from Job1's Reducer            
            // Note: Key-value from a Reducer is separated by a tab
            String[] fields = value.toString().split("\t");
            
            // Extract the location and user count
            String location = fields[0];
            int userCount = Integer.parseInt(fields[1]);
            
            // Save the max user count, and the location
            if (userCount > maxUsers) {
                maxUsers = userCount;
                locationWithMaxUsers = location;
            }
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // This method is called when this task is done.
            // For each task, output is the location having the maximum user count
            // (within the data split)
            if (locationWithMaxUsers != null) {
                outKey.set(maxUsers);
                outValue.set(locationWithMaxUsers);
                context.write(outKey, outValue);
            }
        }
    }
	
	/**
	 * Reducer: get the first result which is the location having maximum
	 * number of users.
	 * 
	 * Note: output from the mapper is sorted in descending order of keys
	 * (user count)
	 */
	public static class MaxCountReducer extends
    Reducer<IntWritable, Text, Text, IntWritable> {
    
        // A flag to see if the maximum value is found
        private boolean foundMax = false;
            
        private IntWritable outValue = new IntWritable();
        
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
                        
            if (!foundMax) {
                // Only use the first Key-Value which contains the answer for the problem
                foundMax = true;
                
                // the first record is the list of locations having the same maximum user count
                outValue.set(key.get());
                for (Text value: values) {
                    context.write(value, outValue);
                }
            }
        }
    }
}
