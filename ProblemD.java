/*
	Find the display name and number of comments done by the user who has got maximum reputation.
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

public class ProblemD extends Configured implements Tool {
    
    // Temporary output folder for joining Users and Posts data
    private static final Path TEMP_OUTPUT = new Path("temp-D");

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new ProblemD(), args);
		System.exit(res);
	}

	/**
     * Configure MapReduce (MR) jobs for a MR program to solve problem D 
     * of the StackOverflow data.
     */
	public int run(String[] args) throws Exception {
		// Command-line arguments for running this MR program:
        // Path (in HDFS) to a CSV file or directory of CSV files of User data
        Path usersPath = new Path(args[0]);
        // Path (in HDFS) to a CSV file or directory of CSV files of Comment data
        Path commentsPath = new Path(args[1]);
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
		 * - Join Users and Comments data to find number of comments for each user.
		 * 
		 * Mapper class: UsersMapper and CommentsMapper
		 * Reducer class: UserCommentReducer
		 */
		Job job1 = new Job(conf, "ProbD-Job1");
		job1.setJarByClass(ProblemD.class);
        
        // Configure classes of key-value output from a Mapper task
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
		
        // Tell Hadoop create a Mapper task using UsersMapper class to process
        // each data split from an input file under the usersPath.
        MultipleInputs.addInputPath(job1, usersPath, TextInputFormat.class, UsersMapper.class);
        // Tell Hadoop create a Mapper task using CommentsMapper class to process
        // each data split from an input file under the commentsPath.
        MultipleInputs.addInputPath(job1, commentsPath, TextInputFormat.class, CommentsMapper.class);
        // Tell Hadoop to create a Reducer task using UserCommentReducer class
        job1.setReducerClass(UserCommentReducer.class);

        // Output of Job1 to the temporary output directory
		FileOutputFormat.setOutputPath(job1, TEMP_OUTPUT); 
		
        // Tell Hadoop to wait for Job1 completed before running Job2
		job1.waitForCompletion(true);
		
		/*
		 * Job 2: From job1's output, find the user with maximum reputation
		 * 
		 * Mapper class:  MaxRepMapper
		 * Reducer class: MaxRepReducer
		 */

		Job job2 = new Job(conf, "ProbD-Job2");
        job2.setJarByClass(ProblemD.class);

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
        // the first record comes to the reducer is the maximum reputation
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
	 * Mapper to read users data file to find user with the local maximum 
	 * reputation (among of users processed by a certain mapper task).
	 * 
	 * Output: one Key-Value pair per mapper task
	 *     Key: UserID
	 *     Value: A text of format "U,<UserName>,<Reputation>"
	 *     (Note: prefix U is to differentiate with output from CommentsMapper)
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
                        // Prefix "U" to differentiate output from CommentsMapper tasks                        
                        new Text("U," + maxRepUser.displayName + "," + maxRepUser.reputation));
            }
        }
	}
	
	/**
     * Mapper to read comments data file to count number of comments per user.
     * 
     * Output: one Key-Value pair per record
     *     Key: UserID
     *     Value: a string "C" as a marker to count a comment for a user
     *     (Note: C is to differentiate with output from UsersMapper)
     */
	public static class CommentsMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse an input line for details of a Comment
            InputParser.Comment comment = InputParser.parseComment(value.toString());
            // Output key is user id of the comment's owner so that
            // reducer can count number of comments for each user.
            // Prefix "C" to differentiate output from UsersMapper tasks 
            context.write(new IntWritable(comment.userId), new Text("C"));
        }
    }
	
	/**
	 * Reducer to join output from UsersMapper and CommentsMapper to find
	 * user reputation, display name, and number of comments.
	 *
	 * Output: one per user (who with the local maximum reputation)
	 *     Key: Reputation value
	 *     Value: A text of format "<UserName>,<CommentCount>"
	 */
	public static class UserCommentReducer extends
    Reducer<IntWritable, Text, IntWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Key-Value are from both UsersMapper and CommentsMapper.
            // UsersMapper: -> (userId, displayname, reputation) 
            // PostsMapper: -> (userId, commentCount)
            // Now joining two tuples above using userId to find 
            //      (reputation, displayname, commentCount)
            String displayName = null;
            int reputation = 0;
            int commentCount = 0;
            
            for (Text value: values) {
                
                // A value string is in CSV format
                String str = value.toString();
                String[] fields = str.split(",");
                
                // Check the prefix to see from which mapper the value is collected
                if (fields[0].equals("U")) {
                    // Values from UsersMapper: get user's display name and reputation
                    displayName = InputParser.getText(fields, 1);
                    reputation = InputParser.getInt(fields, 2);
                    
                } else if (fields[0].equals("C")) {
                    // Values from CommentsMapper: count number of comments per user
                    commentCount++;
                }
            }
            
            if (displayName != null) {
                // For each user, output the reputation, display name, and number of comments                
                context.write(new IntWritable(reputation), new Text(displayName + "," + commentCount));
            }
        }
	}
	
	/**
	 * Mapper: parse job1's output
	 * 
	 * Output: One key-value pair per record
	 *     Key: Reputation
	 *     Value: A text of format "<UserName>,<CommentCount>"
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
            
            // Get the user's reputation value, username, and comment count 
            int reputation = InputParser.getInt(fields, 0);
            String userAndCommentCount = InputParser.getText(fields, 1);
            
            // Output Key is the user reputation so that after Hadoop sorting
            // phase, the first Key-Value sent to the Reducer contains the maximum
            // reputation
            context.write(new IntWritable(reputation), new Text(userAndCommentCount));
        }
    }
	
	/**
	 * Reducer: get the answer which the first key-value pair from Mapper 
	 * output which contains the maximum reputation and the user info
	 * (name and comment count).
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
                    
                    // For each value, extract the display name and comment count
                    String[] fields = value.toString().split(",");
                    String displayName = InputParser.getText(fields, 0);
                    int commentCount = InputParser.getInt(fields, 1);
                    
                    // Output the user's display name and comment count
                    context.write(new Text(displayName), new IntWritable(commentCount));
                }
            }
        }
    }
}
