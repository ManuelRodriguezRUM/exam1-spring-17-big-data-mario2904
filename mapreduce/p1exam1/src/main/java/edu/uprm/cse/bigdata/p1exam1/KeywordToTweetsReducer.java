package edu.uprm.cse.bigdata.p1exam1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KeywordToTweetsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String retweets = "";

        for (Text value: values) {
            retweets+= value.toString() + ", ";
        }
        String value = retweets.substring(0, retweets.length()-2);

        context.write(key, new Text(value));
    }
}
