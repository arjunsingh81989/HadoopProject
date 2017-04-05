import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Utility class to parse StackOverflow data, including User, Post,
 * and Comment data.
 */
public class InputParser {
    
    // To parse date string of format YYYY-MM-DD HH:MM:SS
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("y-M-d H:m:s");

    /**
     * Represent a record in Comment data.
     */
    public static class Comment {
        public int id;
        public int userId;
    }

    /**
     * Represent a record in Post data.
     */
    public static class Post {
        public int id;
        public int postType;
        public long timestamp;
        public int score;
        public int viewCount;
        public int ownerId;
        public String title;
        public int answerCount;
        public int commentCount;
    }
    
    /**
     * Represent a record in PostType data.
     */
    public static class PostType {
        public int id;
        public String name;
    }

    /**
     * Represent a record in User data.
     * Note: Implementing Serializable is not necessary. I intended to use
     * to use User objects as WritableObject.
     */
    public static class User implements Serializable {
        private static final long serialVersionUID = 8212042039599545371L;
        
        public int id;
        public int reputation;
        public String displayName;
        public String location;
        public int age;
    }
    
    /**
     * Parse a CSV line to extract data of a comment (ID, userid)
     * @param line in Comment data file 
     * @return a Comment object
     */
    public static Comment parseComment(String line) {
        
        String[] fields = line.split(",");
        Comment comment = new Comment();
        
        comment.id = getInt(fields, 0);
        comment.userId = getInt(fields, 1);
        
        return comment;
    }
 
    /**
     * Parse a CSV line to extract data of a user (ID, name, etc)
     * @param line in User data file 
     * @return a User object
     */
    public static User parseUser(String line) {
        
        String[] fields = line.split(",");
        User user = new User();
        
        user.id = getInt(fields, 0);
        user.reputation = getInt(fields, 1);
        user.displayName = getText(fields, 2);
        user.location = getText(fields, 3);
        user.age = getInt(fields, 4);
        
        return user;
    }
    
    /**
     * Parse a CSV line to extract data of a post (ID, post type, time, etc)
     * @param line in Post data file 
     * @return a Post object
     */
    public static Post parsePost(String line)  {
        
        String[] fields = line.split(",");
        
        Post post = new Post();
        
        post.id = getInt(fields, 0);
        post.postType = getInt(fields, 1);
        post.timestamp = getTimestamp(fields, 2);
        post.score = getInt(fields, 3);
        post.viewCount = getInt(fields, 4);
        post.ownerId = getInt(fields, 5);
        post.title = getText(fields, 6);
        post.answerCount = getInt(fields, 7);
        post.commentCount = getInt(fields, 8);
        
        return post;
    }
    
    /**
     * Get the i-th field in the fields list as an Int value
     * @param fields array of strings
     * @param index of the string in the array to be parsed 
     * @return Int value represented by the i-th string in the array;
     *         return 0 if the string is not valid, or index is out of range;
     */
    public static int getInt(String[] fields, int index) {
        try {
            return Integer.parseInt(fields[index]);
        } catch (Exception e) {
            return 0;
        }
    }
    
    /**
     * Get the i-th field in the fields list as a Long value
     * @param fields array of strings
     * @param index of the string in the array to be parsed 
     * @return Long value represented by the i-th string in the array;
     *         return 0 if the string is not valid, or index is out of range;
     */
    public static long getLong(String[] fields, int index) {
        try {
            return Long.parseLong(fields[index]);
        } catch (Exception e) {
            return 0;
        }
    }
    
    /**
     * Get the i-th string in the fields list
     * @param fields array of strings
     * @param index of the string in the array to be retrieved 
     * @return the i-th string in the array;
     *         return "" if index is out of range;
     */
    public static String getText(String[] fields, int index) {
        try {
            return fields[index].trim();
        } catch (Exception e) {
            return "";
        }
    }
    
    /**
     * Parse the i-th field in the fields list as date string of format
     * YYYY-MM-DD HH:MM:SS. Return a long value representing the timestamp
     * of the parsed date.
     * @param fields array of strings
     * @param index of the string in the array to be parsed 
     * @return Long value represented by the i-th string in the array;
     *         return -1 if the string is not valid, or index is out of range;
     */
    public static long getTimestamp(String[] fields, int index) {
        try {
            return dateFormat.parse(fields[index]).getTime();
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Format a timestamp value into a date string of format 
     * YYYY-MM-DD HH:MM:SS
     * @param ts Long value representing timestamp of a date
     * @return date string of format YYYY-MM-DD HH:MM:SS
     */
    public static String getDateString(long ts) {
        return dateFormat.format(new Date(ts));
    }
}
