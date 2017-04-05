/*
	Find the title and owner name of the post which has maximum number of Answer Count. 
	Find the title and owner name of post who has got maximum number of Comment count.
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

public class ProblemG extends Configured implements Tool {
    
    // Temporary output folder for joining Users and Posts data
    private static final Path TEMP_OUTPUT = new Path("temp-G");

	public static void main(String args[]) throws Exception {
		int res = ToolRunner.run(new ProblemG(), args);
		System.exit(res);
	}

    /**
     * Configure MapReduce (MR) jobs for a MR program to solve problem G 
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
         * - Read posts data to find post with local maximum answer count,
         *  and post with local maximum comment count.
         * - Join wiht users data for owner name
		 * 
		 * Mapper class: UsersMapper, and PostsMapper
		 * Reducer class: UserReducer
		 */
		Job job1 = new Job(conf, "ProbG-Job1");
		job1.setJarByClass(ProblemG.class);
        
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
		 * Job 2: From job1's output, find:
		 * - the title and owner name of post with maximum answer count
		 * - the title and owner name of post with maximum comment count
		 * 
		 * Mapper class:  MaxCountMapper
		 * Reducer class: MaxCountReducer
		 */

		Job job2 = new Job(conf, "ProbG-Job2");
        job2.setJarByClass(ProblemG.class);

        // Configure classes of key-value output from a Mapper task
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        // Configure classes of key-value output from a Reducer task
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        // Tell Hadoop to create each Mapper task using MaxCountMapper class
        job2.setMapperClass(MaxCountMapper.class);        
        // Tell Hadoop to create each Reducer task using MaxCountReducer class
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
     * Mapper to read users data file to find display name of each user
     * 
     * Output: one Key-Value pair per record
     *     Key: UserID
     *     Value: A text of format "U,<UserName>"
     *     (Note: prefix U is to differentiate with output from other mappers)
     */
    public static class UsersMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            InputParser.User user = InputParser.parseUser(value.toString());
            
            // Only extract display name per user (id)
            outKey.set(user.id);
            outValue.set("U," + user.displayName); // Use prefix "U" to differentiate with other mapper
            context.write(outKey, outValue);
        }
    }
	   
    /**
     * Mapper to read posts data file to find post with local maximum answer 
     * count, and local maximum comment count.
     * 
     * Output: two Key-Value pair per mapper task
     *     Key1: UserID
     *     Value1: a string "A,<AnswerCount>,<PostTitle>"
     *     Key2: UserID
     *     Value2: a string "C,<CommentCount>,<PostTitle>"
     */
    public static class PostsMapper 
    extends Mapper<LongWritable, Text, IntWritable, Text> {
        
        // Get maximum number of answers in the data split (local maximum value)
        // fed to this instance of mapper
        private int maxAnswers = 0;
        
        // Store the users who has the maximum post with maximum answer count
        private int ownerOfMaxAnswers = 0;
        
        // Also the title of the post having the max answers
        private String titleOfMaxAnswers = null;
        
        // Get maximum number of comments in the data split (local maximum value)
        // fed to this instance of mapper
        private int maxComments = 0;
        
        // Store the users who has post having the maximum comments
        private int ownerrWithMaxComments = 0;
        
        // Also the title of the post having the max comments
        private String titleOfMaxComments = null;
        
        private IntWritable outKey = new IntWritable();
        private Text outValue = new Text();
        
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            InputParser.Post post = InputParser.parsePost(value.toString());
            if (post.ownerId <= 0) {
                return; // ignore record without ownerId
            }
            
            // Save the post having the maximum answers
            if (post.answerCount > maxAnswers) {
                maxAnswers = post.answerCount;
                ownerOfMaxAnswers = post.ownerId;
                titleOfMaxAnswers = post.title;
            }
            
            // Save the post having the maximum comments
            if (post.commentCount > maxComments) {
                maxComments = post.commentCount;
                ownerrWithMaxComments = post.ownerId;
                titleOfMaxComments = post.title;
            }
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                throws IOException, InterruptedException {
            
            // This method is called when this task is done.
            
            // Output the post having the maximum answers (local maximum value)
            if (ownerOfMaxAnswers > 0) {
                outKey.set(ownerOfMaxAnswers);
                outValue.set("A," + maxAnswers + "," + titleOfMaxAnswers);
                context.write(outKey, outValue);
            }
            
            // Output the post having the maximum comments (local maximum value)
            if (ownerrWithMaxComments > 0) {
                outKey.set(ownerrWithMaxComments);
                outValue.set("C," + maxComments + "," + titleOfMaxComments);
                context.write(outKey, outValue);
            }
        }
    }
	
	/**
	 * Reducer to join output from UsersMapper, and PostsMapper to get name
	 * of owner of posts with local maximum answer count and local maximum 
	 * comment count.
	 *
	 * Output: two Key-Value pairs per user
	 *     Key1: A text of format "P<PostCount>"
	 *     Value1: <OwnerName>,<PostTitle>
     *     Key2: A text of format "C<CommentCount>"
     *     Value2: <OwnerName>,<PostTitle>
	 */
	public static class UserReducer extends
    Reducer<IntWritable, Text, Text, Text> {
	    
	    private Text outKey = new Text();
	    private Text outValue = new Text();

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values,
                Reducer<IntWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Key-Value are from UsersMapper and PostsMapper.
            // UsersMapper: -> (userId, displayname) 
            // PostsMapper: -> (userId, maxCommentCount, postTitle)
            //             and (userId, maxAnserCount, postTitle)
            // Now joining two tuples above using userId to find 
            //      (displayname, titleOfPostHavingMaxComments)
            //  and (displayname, titleOfPostHavingMaxAnswers)
            
            String ownerName = null;

            // Keep track of post with maximum answers of the Key user
            String titleOfMaxAnswers = null;
            int maxAnswers = -1;
            
            // Keep track of post with maximum comments of the Key user
            String titleOfMaxComments = null;
            int maxComments = -1;
            
            for (Text value: values) {
                
                String str = value.toString();
                
                // Check the prefix to see from which mapper the value is collected
                if (str.startsWith("U,")) {
                    // Values from UsersMapper: get user's display name
                    ownerName = str.substring(2);
                
                } else if (str.startsWith("A,")) {
                    // Values from PostsMapper for answer count
                    String[] fields = str.split(",");
                    int answerCount = Integer.parseInt(fields[1]);
                    String postTitle = fields[2];
                    
                    // Save the max answer count, and the title of the post having that count number
                    if (answerCount > maxAnswers) {
                        maxAnswers = answerCount;
                        titleOfMaxAnswers = postTitle;
                    }
                    
                } else if (str.startsWith("C,")) {
                    // Values from PostsMapper for comment count
                    String[] fields = str.split(",");
                    int commentCount = Integer.parseInt(fields[1]);
                    String postTitle = fields.length > 2 ? fields[2] : "";
                    
                    // Save the max comment count, and the title of the post having that count number
                    if (commentCount > maxComments) {
                        maxComments = commentCount;
                        titleOfMaxComments = postTitle;
                    }
                }
            }
            
            if (ownerName != null) {
                // For each Key user, output two key-value pair:
                
                // First Key-Value: maximum answer count -> owner and the post title
                if (maxAnswers >= 0) {
                    outKey.set("A" + maxAnswers);
                    outValue.set(ownerName + "," + titleOfMaxAnswers);
                    context.write(outKey, outValue);
                }
                
                // Second Key-Value: maximum comment count -> owner and the post title
                if (maxComments >= 0) {
                    outKey.set("C" + maxComments);
                    outValue.set(ownerName + "," + titleOfMaxComments);
                    context.write(outKey, outValue);
                }
            }
        }
	}
	
	/**
	 * Mapper: parse job1's output to find the title and owner name of post:
	 * - With local maximum answer count (amount records processed by an
	 *   individual mapper task)
	 * - With local maximum comment count (amount records processed by an
     *   individual mapper task)
	 * 
	 * Output: Two key-value pairs per task
	 *     Key1: A text of format "A<AnswerCount>"
     *     Value1: <OnwerName>,<PostTitle>
     *     Key2: A text of format "C<CommentCount>"
     *     Value2: <OnwerName>,<PostTitle>
     *  (Note: AnswerCount and CommentCount are output in a fixed-width format
     *  so that they are sorted by the reducers)
	 */
	public static class MaxCountMapper 
    extends Mapper<LongWritable, Text, Text, Text> {
	    
	    // Save the maximum answers count within the data split processed
	    // by a single Mapper task
	    private int maxAnswers = 0;
	    
	    // Additional information of the max answer count: owner name and post title
	    private String infoOfMaxAnswers = null;
	    
	    // Save the maximum comment count within the data split processed
        // by a single Mapper task
	    private int maxComments = 0;
	    
        // Additional information of the max comment count: owner name and post title
	    private String infoOfMaxComments = null;
	    
	    private Text outKey = new Text();
        private Text outValue = new Text();
	    
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            
            // Input is the output from Job1's Reducer            
            // Note: Key-value from a Reducer is separated by a tab
            String[] fields = value.toString().split("\t");
            
            // Check the prefix to see which kind of data is stored
            if (fields[0].startsWith("A")) {
                // Input is answer count, together with title of the post and the post owner
                int answerCount = Integer.parseInt(fields[0].substring(1));
                
                // Update the maximum answer count and its related info
                if (answerCount > maxAnswers) {
                    maxAnswers = answerCount;
                    infoOfMaxAnswers = fields[1];
                }
                
            } else if (fields[0].startsWith("C")) {
                // Input is comment count, together with title of the post and the post owner
                int commentCount = Integer.parseInt(fields[0].substring(1));
                
                // Update the maximum comment count and its related info
                if (commentCount > maxComments) {
                    maxComments = commentCount;
                    infoOfMaxComments = fields[1];
                }
            }
        }

        @Override
        protected void cleanup(
                Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            
            // This method is called when this task is done.

            // Output the local maximum answer count, and its post title and post owner
            if (infoOfMaxAnswers != null) {
                // As all the keys are sorted by Hadoop Sorting phase,
                // Use format of "A##########" for max answer count value
                // and use format of "C##########" for max comment count value
                // (see the below if-statement) to make the sort in desired order,
                outKey.set(String.format("A%010d", maxAnswers));
                outValue.set(infoOfMaxAnswers);
                context.write(outKey, outValue);
            }
            
            // Output the local maximum comment count, and its post title and post owner
            if (infoOfMaxComments != null) {
                outKey.set(String.format("C%010d", maxComments));
                outValue.set(infoOfMaxComments);
                context.write(outKey, outValue);
            }
        }
    }
	
	/**
	 * Reducer: get the answer for post with maximum number of answer count,
	 * and post with the maximum number of comments
	 * 
	 * Note: output from the mapper is sorted in descending order of keys
	 * (count values)
	 */
	public static class MaxCountReducer extends
    Reducer<Text, Text, Text, Text> {
    
	    private Text outKey = new Text();
	    private Text outValue = new Text();
                
        // A flag to see if the maximum answer count is found
        private boolean foundMaxAnswers = false;
        
        // A flag to see if the maximum comment count is found
        private boolean foundMaxComments = false;
    
        @Override
        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            
            String countString = key.toString();
            
            if (!foundMaxAnswers && countString.startsWith("A")) {
                // Only use the first Key-Value (with prefix A) 
                // which contains the global maximum answer count
                foundMaxAnswers = true;
                
                // output a header line - human-readable
                outKey.set("Title of post having maximum no. of Answer Count");
                outValue.set("Owner Name");
                context.write(outKey, outValue);
                
                for (Text value: values) {
                    // extract title and owner name
                    String[] fields = value.toString().split(",");
                    outValue.set(fields[0]);    // owner name
                    outKey.set(fields.length > 1 ? fields[1] : "<NA>");      // post title
                    context.write(outKey, outValue);
                }
            }
            
            if (!foundMaxComments && countString.startsWith("C")) {
                // Only use the first Key-Value (with prefix C) 
                // which contains the global maximum answer count
                foundMaxComments = true;
                
                // output a header line - human-readable
                outKey.set("Title of post having maximum no. of Comment Count");
                outValue.set("Owner Name");
                context.write(outKey, outValue);
                
                for (Text value: values) {
                    // extract title and owner name
                    String[] fields = value.toString().split(",");
                    outValue.set(fields[0]);    // owner name
                    outKey.set(fields.length > 1 ? fields[1] : "<NA>");      // post title
                    context.write(outKey, outValue);
                }
            }
        }
    }
}
