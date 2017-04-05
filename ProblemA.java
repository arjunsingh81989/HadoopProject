
/*
	Problem A. Find the display name and number of posts created by the user who has got maximum reputation.
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

public class ProblemA extends Configured implements Tool {
    
    // Temporary output folder for joining Users and Posts data
    private static final Path TEMP_OUTPUT = new Path("temp-A");

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new ProblemA(), args);
		System.exit(res);
	}

	/**
	 * Configure MapReduce (MR) jobs for a MR program to solve problem A 
	 * of the StackOverflow data.
	 */
	public int run(String[] args) throws Exception {
	    // Command-line arguments for running this MR program:
	    // Path (in HDFS) to a CSV file or directory of CSV files of User data
		Path usersPath = new Path(args[0]);
        // Path (in HDFS) to a CSV file or directory of CSV files of Post data
		Path postsPath = new Path(args[1]);
		// Path (in HDFS) to store output that contains answer for the problem
		Path outputPath = new Path(args[2]);

		Configuration conf = getConf();

		// Delete temporary output and output folder before running. 
        FileSystem fs = FileSystem.get(conf);
        fs.delete(TEMP_OUTPUT, true);
        fs.delete(outputPath, true);
        
		/*
		 * Job 1:
		 * - Filter users data to find the user who has the local maximum 
		 *   reputation (among users processed by a single mapper's task)
		 * - Join Users and Posts data to find number of posts for each user.
		 * 
		 * Mapper class: UsersMapper and PostsMapper
		 * Reducer class: UserPostReducer
		 */
		Job job1 = new Job(conf, "ProbA-Job1");
		job1.setJarByClass(ProblemA.class);
        
		// Configure classes of key-value output from a Mapper task
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
		
        // Tell Hadoop create a Mapper task using UsersMapper class to process
        // each data split from an input file under the usersPath.
		MultipleInputs.addInputPath(job1, usersPath, TextInputFormat.class, UsersMapper.class);
        // Tell Hadoop create a Mapper task using PostsMapper class to process
        // each data split from an input file under the postsPath.
		MultipleInputs.addInputPath(job1, postsPath, TextInputFormat.class, PostsMapper.class);
		// Tell Hadoop to create a Reducer task using UserPostReducer class
		job1.setReducerClass(UserPostReducer.class);
		
		// Output of Job1 to the temporary output directory
		FileOutputFormat.setOutputPath(job1, TEMP_OUTPUT); 
		
		// Tell Hadoop to wait for Job1 completion before running Job2
		job1.waitForCompletion(true);
		
		/*
		 * Job 2: From job1's output, find the user with maximum reputation
		 * 
		 * Mapper class:  MaxRepMapper
		 * Reducer class: MaxRepReducer
		 */

		Job job2 = new Job(conf, "ProbA-Job2");
        job2.setJarByClass(ProblemA.class);

        // Configure classes of key-value output from a Mapper task
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Tell Hadoop to create each Mapper task using MaxRepMapper class
        job2.setMapperClass(MaxRepMapper.class);        
        // Tell Hadoop to create each Reducer task using MaxRepReducer class
        job2.setReducerClass(MaxRepReducer.class);
        
        // Sort by descending order of keys (reputation values) so that
        // the first record that comes to the reducer is the maximum reputation
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

        // Both input and output are in text format
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        // Input path for Job2 is the output path of Job1
        FileInputFormat.setInputPaths(job2, TEMP_OUTPUT);
        // Output path for Job2 is the final output
        FileOutputFormat.setOutputPath(job2, outputPath);

        // Tell Hadoop to wait for Job2 completion before cleaning up 
        // the MR program
		return job2.waitForCompletion(true) ? 0 : 1;
	}
	
	/**
	 * Mapper to read users data file to find user with the local maximum 
	 * reputation (among users processed by a certain mapper task).
	 * 
	 * Output: one Key-Value pair per mapper task
	 *     Key: UserID
	 *     Value: A text of format "U,<UserName>,<Reputation>"
	 *     (Note: prefix U is to differentiate with output from PostsMapper)
	 */
	public static class UsersMapper 
	extends Mapper<LongWritable, Text, IntWritable, Text> {
	    
	    // Save the maximum reputation value within a data split 
	    // (local maximum value) processed by an instance of this Mapper
	    private int maxReputation = 0;
        
	    // Save the User having the maximum reputation (maxReputation)
        private InputParser.User maxRepUser;

        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse an input line for details of a User
            InputParser.User user = InputParser.parseUser(value.toString());
                        
            // Save the maximum reputation value and user who has the maximum value 
            if (user.reputation > maxReputation) {
                maxReputation = user.reputation;
                maxRepUser = user;
            }
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
           
            // This method is called when this task is done.
            // For each task, output is the user having the local maximum reputation value 
            if (maxRepUser != null) {
                context.write(
                        // Key is ID of the user having the local maximum reputation
                        new IntWritable(maxRepUser.id), 
                        // Value is user name and reputation
                        // Prefix "U" to differentiate output from PostsMapper tasks
                        new Text("U," + maxRepUser.displayName + "," + maxRepUser.reputation));
            }
        }
	}
	
	/**
     * Mapper to read posts data file to count number of posts per user.
     * 
     * Output: one Key-Value pair per record
     *     Key: UserID
     *     Value: a string "P" as a marker to count a post for a user
     *     (Note: P is to differentiate with output from UsersMapper)
     */
	public static class PostsMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse an input line for details of a Post
            InputParser.Post post = InputParser.parsePost(value.toString());
            // Output key is user id of the post's owner so that
            // reducer can count number of posts for each user.
            // Prefix "P" to differentiate output from UsersMapper tasks 
            context.write(new IntWritable(post.ownerId), new Text("P"));
        }
    }
	
	/**
	 * Reducer to join output from UsersMapper and PostsMapper to find
	 * user reputation, display name, and number of posts.
	 *
	 * Output: one per user
	 *     Key: Reputation value
	 *     Value: A text of format "<UserName>,<PostCount>"
	 */
	public static class UserPostReducer extends
    Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Key-Value are from both UsersMapper and PostsMapper.
            // UsersMapper: -> (userId, displayname, reputation) 
            // PostsMapper: -> (userId, postcount)
            // Now joining two tuples above using userId to find 
            //      (reputation, displayname, postcount)
            String displayName = null;
            int reputation = 0;
            int postCount = 0;

            for (Text value: values) {
                
                // A value string is in CSV format
                String str = value.toString();
                String[] fields = str.split(",");
                
                // Check the prefix to see from which mapper the value is collected
                if (fields[0].equals("U")) {
                    // Values from UsersMapper: get user's display name and reputation
                    displayName = InputParser.getText(fields, 1);
                    reputation = InputParser.getInt(fields, 2);
                    
                } else if (fields[0].equals("P")) {
                    // Values from PostsMapper: count number of posts
                    postCount++;
                }
            }
            
            if (displayName != null) {
                // For each user, output the reputation, display name, and number of posts
                context.write(
                        new IntWritable(reputation), 
                        new Text(displayName + "," + postCount));
            }
        }
	}
	
	/**
	 * Mapper: parse job1's output
	 * 
	 * Output: One key-value pair per record
	 *     Key: Reputation
	 *     Value: A text of format "<UserName>,<PostCount>"
	 */
	public static class MaxRepMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
	    
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Input is the output from Job1's Reducer            
            // Note: Key-value from a Reducer is separated by a tab
            String[] fields = value.toString().split("\t");

            // Get the user's reputation value, username, and post count 
            int reputation = InputParser.getInt(fields, 0);
            String userAndPostCount = InputParser.getText(fields, 1);
            
            // Output Key is the user reputation so that after Hadoop sorting
            // phase, the first Key-Value sent to the Reducer contains the maximum
            // reputation
            context.write(new IntWritable(reputation), new Text(userAndPostCount));
        }
    }
	
	/**
	 * Reducer: get the answer which the first key-value pair from Mapper 
	 * output which contains the maximum reputation and the user info
	 * (name and post count).
	 * 
	 * Note: output from the mapper is sorted in descending order of keys
	 * (reputation values)
	 */
	public static class MaxRepReducer extends
    Reducer<IntWritable, Text, Text, IntWritable> {
    
	    // A flag to see if the maximum value is found
        private boolean found = false;
    
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            
            if (!found) {
                // Only use the first Key-Value which contains the answer for the problem
                found = true;
                
                // the first record is the list of users having the same maximum reputation
                for (Text value: values) {
                    
                    // For each value, extract the display name and post count
                    String[] fields = value.toString().split(",");
                    String displayName = InputParser.getText(fields, 0);
                    int postCount = InputParser.getInt(fields, 1);
                    
                    // Output the display name and post count of user who has the
                    // maximum reputation value
                    context.write(new Text(displayName), new IntWritable(postCount));
                }
            }
        }
    }
}
