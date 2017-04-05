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

public class ProblemE extends Configured implements Tool {
    
    // Temporary output folder for joining Users and Posts data
    private static final Path TEMP_OUTPUT = new Path("temp-E");

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new ProblemE(), args);
		System.exit(res);
	}

	/**
     * Configure MapReduce (MR) jobs for a MR program to solve problem E 
     * of the StackOverflow data.
     */
	public int run(String[] args) throws Exception {
        // Command-line arguments for running this MR program:
        // Path (in HDFS) to a CSV file or directory of CSV files of User data
		Path usersPath = new Path(args[0]);
        // Path (in HDFS) to a CSV file or directory of CSV files of Post data
		Path postsPath = new Path(args[1]);
        // Path (in HDFS) to a CSV file or directory of CSV files of Comment data
		Path commentsPath = new Path(args[2]);
        // Path (in HDFS) to store output that contains answer for the problem
		Path outputPath = new Path(args[3]);

		Configuration conf = getConf();

        // Delete temporary output and output folder before running. 
        FileSystem fs = FileSystem.get(conf);
        fs.delete(TEMP_OUTPUT, true);
        fs.delete(outputPath, true);
        
		/*
		 * Job 1:
         * - Join Users and Posts data to find number of posts for each user.
		 * - Join Users and Comments data to find number of comments for each user.
		 * 
		 * Mapper class: UsersMapper, PostsMapper, and CommentsMapper
		 * Reducer class: UserReducer
		 */
		Job job1 = new Job(conf, "ProbE-Job1");
		job1.setJarByClass(ProblemE.class);
        
        // Configure classes of key-value output from a Mapper task
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
		
        // Tell Hadoop create a Mapper task using UsersMapper class to process
        // each data split from an input file under the usersPath.
		MultipleInputs.addInputPath(job1, usersPath, TextInputFormat.class, UsersMapper.class);
        // Tell Hadoop create a Mapper task using PostsMapper class to process
        // each data split from an input file under the postsPath.
		MultipleInputs.addInputPath(job1, postsPath, TextInputFormat.class, PostsMapper.class);
        // Tell Hadoop create a Mapper task using CommentsMapper class to process
        // each data split from an input file under the commentsPath.
		MultipleInputs.addInputPath(job1, commentsPath, TextInputFormat.class, CommentsMapper.class);
        // Tell Hadoop to create a Reducer task using UserReducer class
		job1.setReducerClass(UserReducer.class);
		
        // Output of Job1 to the temporary output directory
		FileOutputFormat.setOutputPath(job1, TEMP_OUTPUT); 
		
        // Tell Hadoop to wait for Job1 completed before running Job2
		job1.waitForCompletion(true);
		
		/*
		 * Job 2: From job1's output, find:
		 * - the user with maximum number of posts
		 * - the user with maximum number of comments
		 * 
		 * Mapper class:  MaxCountMapper
		 * Reducer class: MaxCountReducer
		 */

		Job job2 = new Job(conf, "ProbE-Job2");
        job2.setJarByClass(ProblemE.class);

        // Configure classes of key-value output from a Mapper task
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Tell Hadoop to create each Mapper task using MaxCountMapper class
        job2.setMapperClass(MaxCountMapper.class);        
        // Tell Hadoop to create each Reducer task using MaxRepReducer class
        job2.setReducerClass(MaxCountReducer.class);
        
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
	 * Mapper to read users data file to find display name of each user
	 * 
	 * Output: one Key-Value pair per record
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
            
            // Output display name for each user
            // (Note: Prefix U to differentiate output from other mappers
            userId.set(user.id);                // Key is user ID
            text.set("U," + user.displayName);  // Value is displayname
            context.write(userId, text);
        }
	}
	   
    /**
     * Mapper to read posts data file to count number of posts per user.
     * 
     * Output: one Key-Value pair per record
     *     Key: UserID
     *     Value: a string "P" as a marker to count a post for a user
     *     (Note: P is to differentiate with output from other mappers)
     */
    public static class PostsMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        private IntWritable userId = new IntWritable();
        
        // User "P" to differentiate output from UsersMapper and CommentsMapper tasks 
        private Text counter = new Text("P");
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse an input line for details of a User
            InputParser.Post post = InputParser.parsePost(value.toString());
            
            // Output key is user id of the post's owner so that
            // reducer can count number of posts for each user.
            userId.set(post.ownerId);
            context.write(userId, counter);
        }
    }

	/**
     * Mapper to read comments data file to count number of comments per user.
     * 
     * Output: one Key-Value pair per record
     *     Key: UserID
     *     Value: a string "C" as a marker to count a comment for a user
     *     (Note: C is to differentiate with output from other mappers)
     */
	public static class CommentsMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
	    private IntWritable userId = new IntWritable();
        
        // User "C" to differentiate output from UsersMapper and PostsMapper tasks 
        private Text counter = new Text("C");
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            InputParser.Comment comment = InputParser.parseComment(value.toString());
            userId.set(comment.userId);
            
            // Output key is user id of the post's owner so that
            // reducer can count number of posts for each user.
            context.write(userId, counter);
        }
    }
	
	/**
	 * Reducer to join output from UsersMapper, PostsMapper, and CommentsMapper
	 * to count number of posts and number of comments per user
	 *
	 * Output: two Key-Value pairs per user
	 *     Key1: A text of format "P<PostCount>"
	 *     Value1: User's display name
     *     Key2: A text of format "C<CommentCount>"
     *     Value2: User's display name
	 */
	public static class UserReducer extends
    Reducer<IntWritable, Text, Text, Text> {
	    
	    private Text outKey = new Text();
	    
	    private Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Key-Value are from UsersMapper, PostsMapper, and CommentsMaper
            // UsersMapper: -> (userId, displayname) 
            // PostsMapper: -> (userId, postcount)
            // CommentsMaper: -> (userId, commentcount)
            // Now joining two tuples above using userId to find 
            //      (displayname, postcount)
            // and
            //      (displayname, commentcount)
            String displayName = null;
            int postCount = 0;
            int commentCount = 0;
            
            for (Text value: values) {
                
                String str = value.toString();
                    
                // Check the prefix to see from which mapper the value is collected
                if (str.equals("P")) {
                    // Values from PostsMapper: count number of posts per user
                    postCount++;
                    
                } else if (str.equals("C")) {
                    // Values from CommentsMapper: count number of comments per user
                    commentCount++;
                } else {
                    // Values from UsersMapper: get user's display name
                    displayName = str.substring(2);
                }
            }
            
            if (displayName != null) {
                outValue.set(displayName);
                
                // output number of comments of the user
                outKey.set("C" + commentCount);
                context.write(outKey, outValue);
                
                // output number of posts of the user
                outKey.set("P" + postCount);
                context.write(outKey, outValue);
            }
        }
	}
	
	/**
	 * Mapper: parse job1's output to find display name of user:
	 * - With local maximum number of posts (amount records processed by an
	 *   individual mapper task)
	 * - With local maximum number of comments (amount records processed by an
     *   individual mapper task)
	 * 
	 * Output: Two key-value pairs per task
	 *     Key1: A text of format "P<PostCount>"
     *     Value1: User's display name
     *     Key2: A text of format "C<CommentCount>"
     *     Value2: User's display name
     *  (Note: PostCount and CommentCount are output in a fixed-width format
     *  so that they are sorted by the reducers)
	 */
	public static class MaxCountMapper 
    extends Mapper<LongWritable, Text, Text, Text> {
	    
	    // Get maximum number of posts in the data split (local maximum value)
	    // fed to this instance of mapper
	    private int maxPosts = 0;
	    
	    // Store the users who has the maximum post count above
	    private String userWithMaxPosts = null;
	    
        // Get maximum number of comments in the data split (local maximum value)
        // fed to this instance of mapper
	    private int maxComments = 0;
	    
        // Store the users who has the maximum comment count above
	    private String userWithMaxComments = null;
	    
	    private Text outKey = new Text();
        
        private Text outValue = new Text();
	    
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Input from previous job, data is spearated by tabs
            String[] fields = value.toString().split("\t");
            
            // Check to see the input is comment count or post count
            if (fields[0].startsWith("C")) {
                // Input is comment count value
                int commentCount = Integer.parseInt(fields[0].substring(1));
                
                // Keep track of the maximum comments and the user having the maximum comments
                if (commentCount > maxComments) {
                    maxComments = commentCount;
                    userWithMaxComments = fields[1];
                }
                
            } else if (fields[0].startsWith("P")) {
                // Input is comment post value
                int postCount = Integer.parseInt(fields[0].substring(1));
                
                // Keep track of the maximum post and the user having the maximum post
                if (postCount > maxPosts) {
                    maxPosts = postCount;
                    userWithMaxPosts = fields[1];
                }
            }
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            
            // This method is called when this task is done.

            // Output is the user having the local maximum comments 
            if (userWithMaxComments != null) {
                outKey.set(String.format("C%010d", maxComments));
                outValue.set(userWithMaxComments);
                context.write(outKey, outValue);
            }
            
            // Output is the user having the local maximum posts 
            if (userWithMaxPosts != null) {
                outKey.set(String.format("P%010d", maxPosts));
                outValue.set(userWithMaxPosts);
                context.write(outKey, outValue);
            }
        }
    }
	
	/**
	 * Reducer: get the answer for user with maximum number of posts,
	 * and user with the maximum number of comments
	 * 
	 * Note: output from the mapper is sorted in descending order of keys
	 * (count values)
	 */
	public static class MaxCountReducer extends
    Reducer<Text, Text, Text, Text> {
    
	    private Text outKey = new Text();
                
        // A flag to see if the maximum posts is found
        private boolean foundMaxPosts = false;
        
        // A flag to see if the maximum posts is found
        private boolean foundMaxComments = false;
    
        @Override
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            
            String countString = key.toString();
            
            if (!foundMaxPosts && countString.startsWith("P")) {
                // Only use the first Key-Value (of prefix P) 
                // which contains the maximum posts
                foundMaxPosts = true;
                
                outKey.set("User with maximum no. of posts:");
                for (Text value: values) {
                    context.write(outKey, value);
                }
            }
            
            if (!foundMaxComments && countString.startsWith("C")) {
                // Only use the first Key-Value (of prefix C) 
                // which contains the maximum comments
                foundMaxComments = true;
                
                outKey.set("User with maximum no. of comments:");
                for (Text value: values) {
                    context.write(outKey, value);
                }
            }
        }
    }
}
