package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.IOException;

public class KeywordToTweetsMapper extends Mapper<LongWritable, Text, Text, Text> {

    public static final String[] TRUMP_WORDS = new String[] {"MAGA", "DICTATOR", "IMPEACH", "DRAIN", "SWAMP", "CHANGE"};

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String rawTweet = value.toString();

        try {
            Status tweetStatus = TwitterObjectFactory.createStatus(rawTweet);
            String statusId = String.valueOf(tweetStatus.getId());
            String rawText = tweetStatus.getText().toUpperCase();
            for (String word: TRUMP_WORDS) {
                if(rawText.contains(word))
                    context.write(new Text(word), new Text(statusId));
            }
        }
        catch(TwitterException e){
            // Ignore bad tweets
            Logger logger = LogManager.getRootLogger();
            logger.trace("Bad Tweet: " + rawTweet);
        }

    }

}
