/*
	Find the owner name and id of user whose post has got maximum number of view counts so far.
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

public class ProblemF extends Configured implements Tool {
    
    // Temporary output folder for joining Users and Posts data
    private static final Path TEMP_OUTPUT = new Path("temp-F");

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new ProblemF(), args);
		System.exit(res);
	}

	/**
     * Configure MapReduce (MR) jobs for a MR program to solve problem F
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
		 * - Find posts with local maximum number of view count (amount posts
		 *   processed by an individual mapper task)
         * - Join Users and Posts data to display name of the post owner
		 * 
		 * Mapper class: UsersMapper, and PostsMapper,
		 * Reducer class: UserReducer
		 */
		Job job1 = new Job(conf, "ProbF-Job1");
		job1.setJarByClass(ProblemF.class);
        
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
		job1.setReducerClass(UserReducer.class);
		
        // Output of Job1 to the temporary output directory
		FileOutputFormat.setOutputPath(job1, TEMP_OUTPUT); 
		
        // Tell Hadoop to wait for Job1 completed before running Job2
		job1.waitForCompletion(true);
		
		/*
		 * Job 2: From job1's output, find the post with global maximum view 
		 * count along with the owner display name and id
		 * 
		 * Mapper class:  MaxCountMapper
		 * Reducer class: MaxCountReducer
		 */

		Job job2 = new Job(conf, "ProbF-Job2");
        job2.setJarByClass(ProblemF.class);

        // Configure classes of key-value output from a Mapper task
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        // Tell Hadoop to create each Mapper task using MaxCountMapper class
        job2.setMapperClass(MaxCountMapper.class);        
        // Tell Hadoop to create each Reducer task using MaxCountReducer class
        job2.setReducerClass(MaxCountReducer.class);
        
        // Sort by descending order of keys (view count values) so that
        // the first record comes to the reducer is the maximum view count
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
	 *     Value: A text of format "U,<UserName>"
	 *     (Note: prefix U is to differentiate with output from other mappers)
	 */
	public static class UsersMapper 
	extends Mapper<LongWritable, Text, IntWritable, Text> {
	    
	    private IntWritable userId = new IntWritable();
	    
	    private Text text = new Text();
	    
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse an input line for details of a User
            InputParser.User user = InputParser.parseUser(value.toString());
            
            // make user record is valid
            if (user.id > 0) {
                // Output the user id and display name
                userId.set(user.id);
                text.set("U," + user.displayName);
                context.write(userId, text);
            }
        }
	}
	   
    /**
     * Mapper to read posts data file to find post with local maximum view 
     * count (among records processed by an individual mapper task)
     * 
     * Output: one Key-Value pair per record
     *     Key: UserID
     *     Value: a string of format "P,<ViewCount>"
     *     (Note: P is to differentiate with output from other mappers)
     */
    public static class PostsMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        // Save the maximum view count value within a data split 
        // (local maximum value) processed by an instance of this Mapper
        private int maxViewCount = 0;
        
        // Save the User having the maximum view count
        private int maxViewOwnerId = 0;
        
        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse an input line for details of a Post
            InputParser.Post post = InputParser.parsePost(value.toString());
            
            // Save the maximum view count value and user who has the maximum value 
            if (post.ownerId > 0 && post.viewCount > maxViewCount) {
                maxViewCount = post.viewCount;
                maxViewOwnerId = post.ownerId;
            }
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // This method is called when this task is done.
            // For each task, output is the user having the local maximum view count value 
            if (maxViewOwnerId > 0) {
                outKey.set(maxViewOwnerId);
                outValue.set("P," + maxViewCount);  // Prefix "P" to differentiate output from UsersMapper tasks
                context.write(outKey, outValue);
            }
        }
    }
	
	/**
	 * Reducer to join output from UsersMapper, and PostsMapper to 
	 * get the owner name of post having local maximum view count
	 *
	 * Output: one Key-Value pair per join
	 *     Key: ViewCount
	 *     Value: Text of format "<DisplayName>,<UserId>"
	 */
	public static class UserReducer extends
    Reducer<IntWritable, Text, IntWritable, Text> {
	    
	    private IntWritable outKey = new IntWritable();
	    private Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Key-Value are from both UsersMapper and PostsMapper.
            // UsersMapper: -> (userId, displayname) 
            // PostsMapper: -> (userId, viewcount)
            // Now joining two tuples above using userId to find 
            //      (viewcount, displayname)
            int userId = key.get();
            String displayName = null;
            int maxViewCount = -1;

            // Find the maximum view count amount the posts posted by the user
            for (Text value: values) {
                
                String str = value.toString();
                    
                if (str.startsWith("U,")) {
                    // Values from UsersMapper: extract user display name
                    displayName = str.substring(2);
                    
                } else if (str.startsWith("P,")) {
                    // Values from PostsMapper: extract post count
                    int viewCount = Integer.parseInt(str.substring(2));
                    if (viewCount > maxViewCount) {
                        maxViewCount = viewCount;
                    }
                }
            }
            
            if (maxViewCount >= 0 && displayName != null) {
                // For each user, output the maximum view count value that the user
                // have - together with user's displayname
                outKey.set(maxViewCount);
                outValue.set(displayName + "," + userId);
                context.write(outKey, outValue);
            }
        }
	}
	
	/**
     * Mapper: parse job1's output
     * 
     * Output: One key-value pair per record
     *     Key: ViewCount
     *     Value: A text of format "<UserName>,<UserId>"
     */
    public static class MaxCountMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Input is the output from Job1's Reducer            
            // Note: Key-value from a Reducer is separated by a tab
            String[] fields = value.toString().split("\t");
            
            int viewCount = Integer.parseInt(fields[0]);
            String userInfo = fields[1];
            
            // Output Key is the local maximum view counts so that after 
            // Hadoop sorting phase, the first Key-Value sent to the Reducer 
            // contains the global maximum view count
            outKey.set(viewCount);
            outValue.set(userInfo);
            context.write(outKey, outValue);
        }
    }
	
	/**
	 * Reducer: get the answer for display name and id of users whose post
	 * got maximum number of view count.
	 * 
	 * Note: output from the mapper is sorted in descending order of keys
	 * (view count values)
	 */
	public static class MaxCountReducer extends
    Reducer<IntWritable, Text, Text, IntWritable> {
    
	    private Text outKey = new Text();
	    private IntWritable outValue = new IntWritable();
                
        // A flag to see if the maximum value is found
        private boolean foundMax = false;
    
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            
            if (!foundMax) {
                // Only use the first Key-Value which contains the answer for the problem
                foundMax = true;
                
                for (Text value: values) {
                    // Extract user's display name and id
                    String[] fields = value.toString().split(",");
                    String userName = fields[0];
                    int userId = Integer.parseInt(fields[1]);
                    
                    // Output the display name of the user having the post with the
                    // maximum view count
                    outKey.set(userName);
                    outValue.set(userId);
                    context.write(outKey, outValue);
                }
            }
        }
    }
}
