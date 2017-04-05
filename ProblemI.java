/*
	Find the total number of answers, posts, and comments created by Indian users.
*/


import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;

public class ProblemI extends Configured implements Tool {
    
    // Temporary output folder for joining Users and Posts data
    private static final Path TEMP_OUTPUT = new Path("temp-I");

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new ProblemI(), args);
		System.exit(res);
	}

    /**
     * Configure MapReduce (MR) jobs for a MR program to solve problem I 
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
         * - Read Users data to find IDs of Indian users
         * - Join with posts data to count posts/answers per Indian user
         * - Join with comments data to count comments per Indian user
		 * 
		 * Mapper class: UsersMapper, PostsMapper, and CommentsMapper
		 * Reducer class: CountReducer
		 */
		Job job1 = new Job(conf, "ProbI-Job1");
		job1.setJarByClass(ProblemI.class);
        
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
        // Tell Hadoop create a Mapper task using CommentsMapper class to process
        // each data split from an input file under the commentsPath.
		MultipleInputs.addInputPath(job1, commentsPath, TextInputFormat.class, CommentsMapper.class);
        // Tell Hadoop to create a Reducer task using UserReducer class
		job1.setReducerClass(CountReducer.class);
		
        // Output of Job1 to the temporary output directory
		FileOutputFormat.setOutputPath(job1, TEMP_OUTPUT); 
		
        // Tell Hadoop to wait for Job1 completed before running Job2
		job1.waitForCompletion(true);
		
		/*
		 * Job 2: From job1's output, count total answers, posts, and comments
		 * created by Indian users
		 * 
		 * Mapper class:  TotalCountMapper
		 * Reducer class: TotalCountReducer
		 */

		Job job2 = new Job(conf, "ProbI-Job2");
        job2.setJarByClass(ProblemI.class);

        // Configure classes of key-value output from a Mapper task
        job2.setMapOutputKeyClass(IntWritable.class);
        job2.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Tell Hadoop to create each Mapper task using TotalCountMapper class
        job2.setMapperClass(TotalCountMapper.class);        
        // Tell Hadoop to create each Reducer task using TotalCountReducer class
        job2.setReducerClass(TotalCountReducer.class);

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
	 * Mapper to read users data file to find IDs of Indian users
	 * 
	 * Output: one Key-Value pair per record
	 *     Key: UserID
	 *     Value: "U"
	 *     (Note: prefix U is to differentiate with output from other mappers)
	 */
	public static class UsersMapper 
	extends Mapper<LongWritable, Text, IntWritable, Text> {
	    
	    // Pattern to find if a location is from India
	    private Pattern pattern = Pattern.compile("\\bindia$", Pattern.CASE_INSENSITIVE);
	    
	    // Output key will contain user id
	    private IntWritable outKey = new IntWritable();
	    
	    // "U" to differentiate output from other mappers
	    private Text outValue = new Text("U");
	    
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse for User data
            InputParser.User user = InputParser.parseUser(value.toString());
            
            // Check if the location is from India
            if (pattern.matcher(user.location).find()) {
                // Output the user found
                outKey.set(user.id);
                context.write(outKey, outValue);
            }
        }
	}
	   
    /**
     * Mapper to read posts data file to count number of answers, posts per user.
     * 
     * Output: one Key-Value pair per record
     *     Key: UserID
     *     Value: a string of format "P,<PostType>"
     *     (Note: P is to differentiate with output from other mappers)
     */
    public static class PostsMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse for Post data
            InputParser.Post post = InputParser.parsePost(value.toString());
            
            // Make sure Onwer Id is valid
            if (post.ownerId > 0) {
                // Output the owner ID, and the type of post
                outKey.set(post.ownerId);
                outValue.set("P," + post.postType); // Use prefix P to differentiate with other mappers
                context.write(outKey, outValue);
            }
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
        
        // Output key will contain user id
        private IntWritable userId = new IntWritable();
        
        // "C" to differentiate output from other mappers 
        private Text counter = new Text("C");
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Parse Comment data
            InputParser.Comment comment = InputParser.parseComment(value.toString());
            // Output the user id and a indicator indicating that a comment is found
            userId.set(comment.userId);
            context.write(userId, counter);
        }
    }
	
	/**
	 * Reducer to join output from UsersMapper, PostsMapper, and CommentsMapper
	 * to count number of posts, answers, and comments per user
	 *
	 * Output: one Key-Value pair per record
	 *     Key: 1
	 *     Value: A text of format "T,<AnswerCount>,<PostCount>,<CommentCount>"
	 */
	public static class CountReducer extends
    Reducer<IntWritable, Text, IntWritable, Text> {
	    
	    // Use arbitrary key as we don't care the key value here 
	    private IntWritable one = new IntWritable(1);
	    
	    private Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Key-Value are from both UsersMapper, PostsMapper, and CommentsMapper
            // UsersMapper: -> (userId, "from India") 
            // PostsMapper: -> (userId, posttype)
            // CommentsMapper: -> (userId, "C")
            // Now joining two tuples above using userId to find 
            //      - number of posts (of any type)
            //      - number of posts which is answer post (posttype = 2)
            //      - number of comments
            // of a user from India
            int postCount = 0;
            int answerCount = 0;
            int commentCount = 0;
            int userId = 0;
            
            for (Text value: values) {
                
                String str = value.toString();
                   
                // Check the value prefix to see which value means
                if (str.equals("U")) {
                    // Values from UsersMapper: key is id of user from India
                    userId = key.get();
                    
                } else if (str.equals("C")) {
                    // Values from CommentsMapper: count number of comments per user
                    commentCount++;
                    
                } else {
                    // Values from PostsMapper: count posts and answers
                    postCount++;
                    
                    // Extract post type
                    int postType = Integer.parseInt(str.substring(2));
                    if (postType == 2) {
                        answerCount++;
                    }
                }
            }
            
            // Make sure we found the user from India
            if (userId > 0) {
                // Output the answer post count, post count, and comment count
                outValue.set("T," + answerCount + "," + postCount + "," + commentCount);
                context.write(one, outValue);
                
                // Note the trick is here:
                // Output is written to file as:
                // 1    T,<answerCount>,<postCount>,<commentCount>
                // We used T, here so that the mapper of Job2 can treat the
                // input as CSV format, the first field "1  T" is ignored
            }
        }
	}
	
	/**
	 * Mapper: parse job1's output to count number of answers, posts, and comments
	 * of Indian users proccessed by an individual mapper task
	 * 
	 * Output: One key-value pair per task
	 *     Key: 1
     *     Value: A text of format "<AnswerCount>,<PostCount>,<CommentCount>"
	 */
	public static class TotalCountMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
	    
	    // To count number of post (within a data split)
	    private int postCount = 0;
	    
	    // To count number of post which is answer post (within a data split)
	    private int answerCount = 0;
	    
	    // To count number of comment (within a data split)
	    private int commentCount = 0;
	    
	    // Use key of one so that all counts value are reduced to a single reducer
	    private IntWritable one = new IntWritable(1);
        
        private Text outValue = new Text();
	    
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {

            // Take output of Job 1 as input, Key-Value is separated by tab
            // However, we treat it as CSV data (see the reason above)
            String[] fields = value.toString().split(",");
            
            // Count number answers, posts, and comments (within a data split)
            answerCount += Integer.parseInt(fields[1]);
            postCount += Integer.parseInt(fields[2]);
            commentCount += Integer.parseInt(fields[2]);
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // This method is called when this task is done.
            // For each task, output is count values (of a single data split)
            outValue.set(answerCount + "," + postCount + "," + commentCount);
            context.write(one, outValue);
        }
    }
	
	/**
	 * Reducer: sum all counts from each mapper task to get total number of
	 * answers, posts, and comments created by Indian users
	 */
	public static class TotalCountReducer extends
    Reducer<IntWritable, Text, Text, Text> {
    
	    // Output key - for human-readable
	    private Text outKey = new Text("Total no. of answers, posts, "
	            + "comments created by Indian users:");
	    
	    private Text outValue = new Text();
    
        @Override
        public void reduce(IntWritable key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            
            // Sum local count values (from each mapper) to get the global 
            // count values
            
            int postCount = 0;
            int answerCount = 0;
            int commentCount = 0;
            
            for (Text value : values) {
                // Each value contains three count values, separeted by commas
                String[] fields = value.toString().split(",");
            
                // Sum each count value
                answerCount += Integer.parseInt(fields[0]);
                postCount += Integer.parseInt(fields[1]);
                commentCount += Integer.parseInt(fields[2]);
            }
            
            // Output the final answer for the problem
            outValue.set(answerCount + "," + postCount + "," + commentCount);
            context.write(outKey, outValue);
        }
    }
}
