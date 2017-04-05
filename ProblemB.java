/*
	Find the average age of users on the Stack Overflow site.
*/

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

public class ProblemB extends Configured implements Tool {
    
	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new ProblemB(), args);
		System.exit(res);
	}

	/**
     * Configure MapReduce (MR) jobs for a MR program to solve problem B 
     * of the StackOverflow data.
     */
	public int run(String[] args) throws Exception {
	    // Command-line arguments for running this MR program:
        // Path (in HDFS) to a CSV file or directory of CSV files of User data
	    Path inputPath = new Path(args[0]);
	    // Path (in HDFS) to store output that contains answer for the problem
		Path outputPath = new Path(args[1]);

		Configuration conf = getConf();
		
		/*
		 * One job is required that does the following task
		 * - Mapper: count each user of an age value
		 * - Combiner: sum all ages and count number users of the same age
		 * - Reducer: finally compute the total ages of all users and 
		 *   total number of users to get the average age value.
		 * 
		 * Mapper class: AgeMapper
		 * Combiner class: AgeSumReducer
		 * Reducer class: AvgAgeReducer
		 */
		Job job = new Job(conf, "ProbB");
		job.setJarByClass(ProblemB.class);
        
        // Configure classes of key-value output from a Mapper task
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        // (applied to both Combiner and Reducer)
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        // Tell Hadoop to create each Mapper task using AgeMapper class
        job.setMapperClass(AgeMapper.class);      
        // Tell Hadoop to create each Reducer task (in Combine phase) 
        // using AgeSumReducer class        
        job.setCombinerClass(AgeSumReducer.class);
        // Tell Hadoop to create each Reducer task using AvgAgeReducer class
        job.setReducerClass(AvgAgeReducer.class);

        // Both input and output are in text format
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Get the input and output phat
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        // Tell Hadoop to wait for the job completed before cleaning up 
        // the MR program
		return job.waitForCompletion(true) ? 0 : 1;
	}
	
	/**
	 * Mapper to read user data to count number of users of each
	 * age value.
	 * 
	 * Output: one Key-Value pair per input
	 *     Key: Age value
	 *     Value: any value to indicate that we have found one user of the Key age
	 */
	public static class AgeMapper 
	extends Mapper<LongWritable, Text, IntWritable, Text> {
	    
	    //Arbitrary for each value - just use an empty string
	    private final Text counter = new Text(); 
	    
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse user data
            InputParser.User user = InputParser.parseUser(value.toString());
            // If user's age is present, count that one user of the age has been found 
            if (user.age > 0) {
                context.write(new IntWritable(user.age), counter);
            }
        }
	}
	
	/**
	 * Reducer class (used in Combining phase) to count number of users of each
	 * distinct age.
	 * 
	 * Output: one Key-Value per input
	 *     Key: a chosen key so that all values are combined into the same Reducer
	 *     Value: A text of format "<SumAge,UserCount>" where
	 *         SumAge: is the sum of ages of all users having the same age
	 *         UserCount: number of users having the same age
	 */
	public static class AgeSumReducer extends
    Reducer<IntWritable, Text, IntWritable, Text> {
	    
	    // A chosen value (1) used a the output key
	    private final IntWritable one = new IntWritable(1); 
        
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Input Key is a distinct value of age
            // Values is a list of counter, each indicates that one user having the age
            
            // Count number of users having the same age
            int count = 0;
            for (Iterator<Text> itor = values.iterator(); itor.hasNext(); itor.next())  {
                count++;
            }
            
            // Sum ages of all users who age is the value of the input key
            int sumAge = key.get() * count;
            
            context.write(one, new Text(sumAge + "," + count));
        }
    }
	
	/**
	 * Reducer class (used in Reduce phase) to calculate the average age 
	 * of all users (the final answer for this problem)
	 */
	public static class AvgAgeReducer extends
    Reducer<IntWritable, Text, Text, FloatWritable> {
	    
	    // Output key - for human-readable
	    private final Text averageText = new Text("Average age:");

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, Text, FloatWritable>.Context context)
                throws IOException, InterruptedException {
            
            // Input Key-(value, ...) is from AgeSumReducer
            // Key is all 1
            // Values are a list of (sumage, count)
            
            // Now sum all the "SumAge" to get the total ages of all users
            float totalAge = 0;
            // Sum all the "Count" to get total number of users
            int numUsers = 0;
            
            for (Text value : values) {
                // Extract SumAge and Count from each value
                String[] fields = value.toString().split(",");
                int sumAge = InputParser.getInt(fields, 0);
                int count = InputParser.getInt(fields, 1);
                
                totalAge += sumAge;
                numUsers += count;
            }
            
            // Now the average age is Total of Age / Total Users
            context.write(averageText, new FloatWritable(totalAge / numUsers));
        }
	}
}
