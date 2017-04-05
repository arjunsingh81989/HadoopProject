/*
	Find the display name of user who posted the oldest post on Stack Overflow (in terms of date).
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

public class ProblemC extends Configured implements Tool {
    
    // Temporary output folder for joining Users and Posts data
    private static final Path TEMP_OUTPUT = new Path("temp-C");

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new ProblemC(), args);
		System.exit(res);
	}

	/**
     * Configure MapReduce (MR) jobs for a MR program to solve problem C 
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
		 * - Filter posts data to find the local oldest post (among posts
		 *   processed by a single mapper's task)
		 * - Join Users and Posts data to display name of the local oldest posts
		 * 
		 * Mapper class: UsersMapper and PostsMapper
		 * Reducer class: UserPostReducer
		 */
		Job job1 = new Job(conf, "ProbC-Job1");
		job1.setJarByClass(ProblemC.class);
        
        // Configure classes of key-value output from a Mapper task
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job1.setOutputKeyClass(LongWritable.class);
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
		
	     // Tell Hadoop to wait for Job1 completed before running Job2
		job1.waitForCompletion(true);
		
		/*
		 * Job 2: From job1's output, find the display name of the user who 
		 * posted the oldest post.
		 * 
		 * Mapper class:  OldestPostMapper
		 * Reducer class: OldestPostReducer
		 */

		Job job2 = new Job(conf, "ProbC-Job2");
        job2.setJarByClass(ProblemC.class);

        // Configure classes of key-value output from a Mapper task
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Tell Hadoop to create each Mapper task using OldestPostMapper class
        job2.setMapperClass(OldestPostMapper.class);        
        // Tell Hadoop to create each Reducer task using OldestPostReducer class
        job2.setReducerClass(OldestPostReducer.class);
        
        // Sort by ascending order of keys (timestamp values) so that
        // the first record comes to the reducer is the oldest post
        job2.setSortComparatorClass(LongWritable.Comparator.class);

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
	 * Mapper to read posts data file to find post with the local minimum 
	 * creation date (among of posts processed by a certain mapper task).
	 * 
	 * Output: one Key-Value pair per mapper task
	 *     Key: UserID
	 *     Value: A text of format "P,<CreationTimestamp>"
	 *     (Note: prefix P is to differentiate with output from UsersMapper)
	 */
	public static class PostsMapper 
	extends Mapper<LongWritable, Text, IntWritable, Text> {
	    
	    // Save the oldest post time within a data split 
        // (local maximum value) processed by an instance of this Mapper
	    private long oldestTs = 0;
        
	    // Save the user id of the owner of the oldest post
        private int oldestPostUserId;

        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse an input line for details of a Post
            InputParser.Post post = InputParser.parsePost(value.toString());
                        
            // Save post with the minimum timestamp value
            if (post.timestamp > 0 && (oldestTs == 0 || post.timestamp < oldestTs)) {
                oldestTs = post.timestamp;
                oldestPostUserId = post.ownerId;
            }
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            // This method is called when this task is done.
            // For each task, output is the user id having the local oldest post time 
            if (oldestTs > 0) {
                context.write(
                        // Key is ID of the owner of the oldest post
                        new IntWritable(oldestPostUserId),
                        // Value is the oldest timestamp
                        // Prefix "U" to differentiate output from UsersMapper tasks
                        new Text("P," + oldestTs));
            }
        }
	}
	
	/**
     * Mapper to read users data file for user id and display name
     * 
     * Output: one Key-Value pair per record
     *     Key: UserID
     *     Value: A text of format "U,<UserName>"
     *     (Note: prefix U is to differentiate with output from PostsMapper)
     */
	public static class UsersMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse an input line for details of a User
            InputParser.User user = InputParser.parseUser(value.toString());
            // Output the user id and display name
            // Prefix "U" to differentiate output from PostsMapper tasks
            context.write(new IntWritable(user.id), new Text("U," + user.displayName));
        }
    }
	
	/**
	 * Reducer to join output from UsersMapper and PostsMapper to find
	 * the local oldest post with the owner's display name.
	 *
	 * Output: one per post (which with the local minimum timestamp)
	 *     Key: Post Timestamp value
	 *     Value: Owner's display name
	 */
	public static class UserPostReducer extends
    Reducer<IntWritable, Text, LongWritable, Text> {

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Key-Value are from both UsersMapper and PostsMapper.
            // UsersMapper: -> (userId, displayname) 
            // PostsMapper: -> (userId, oldestTimestamp)
            // Now joining two tuples above using userId to find 
            //      (oldestTimestamp, displayname)
            String userWithOldestPost = null;
            long oldestTs = 0;
            
            for (Text value: values) {
                
                // A value string is in CSV format
                String str = value.toString();
                String[] fields = str.split(",");
                
                if (fields[0].equals("U")) {
                    // Values from UsersMapper: get user's display name
                    userWithOldestPost = InputParser.getText(fields, 1);
                    
                } else if (fields[0].equals("P")) {
                    // Values from PostsMapper: get the oldest post timestamp
                    long ts = InputParser.getLong(fields, 1);
                    if (ts > 0 && (oldestTs == 0 || ts < oldestTs)) {
                        oldestTs = ts;
                    }
                }
            }
            
            if (oldestTs > 0) {
                // For each user, output the timestamp of the oldest post and user display name                
                context.write(new LongWritable(oldestTs), new Text(userWithOldestPost));
            }
        }
	}
	
	/**
	 * Mapper: parse job1's output
	 * 
	 * Output: One key-value pair per record
	 *     Key: Timestamp of a post (with local minimum timestamp)
	 *     Value: Display name of the post ownwer
	 */
	public static class OldestPostMapper 
    extends Mapper<LongWritable, Text, LongWritable, Text> {
	    
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, LongWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Input is the output from Job1's Reducer            
            // Note: Key-value from a Reducer is separated by a tab
            String[] fields = value.toString().split("\t");
            
            // Extract the timestamp value and display name from then value
            long ts = InputParser.getLong(fields, 0);
            String ownerDisplayName = InputParser.getText(fields, 1);
            
            // Output Key is the post's timestamp so that after Hadoop sorting
            // phase, the first Key-Value sent to the Reducer contains the minimum
            // timestamp value 
            context.write(new LongWritable(ts), new Text(ownerDisplayName));
        }
    }
	
	/**
	 * Reducer: get the answer which the first key-value pair from Mapper 
	 * output which contains the display name of the user who posted the
	 * oldest post and the oldest date-time.
	 * 
	 * Note: output from the mapper is sorted in ascending order of keys
	 * (timestamp values)
	 */
	public static class OldestPostReducer extends
    Reducer<LongWritable, Text, Text, Text> {
    
        // A flag to see if the maximum value is found
	    private boolean found = false;
    
        @Override
        public void reduce(LongWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            
            if (!found) {
                // Only use the first Key-Value which contains the answer for the problem
                found = true;
                
                // the first record is the list of users having posts with the same oldest timestamp
                for (Text value: values) {
                    
                    // Output the user's display name who post the oldest post
                    // and the oldest date time
                    context.write(value, new Text(InputParser.getDateString(key.get())));
                }
            }
        }
    }
}
