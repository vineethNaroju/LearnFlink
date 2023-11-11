package org.rough.top_k_tags_in_a_minute;

public class RawTweet {

    final String text;
    final String tweetId;
    final long timestamp;

    public RawTweet(long timestamp, String tweetId, String text) {
        this.text = text;
        this.tweetId = tweetId;
        this.timestamp = timestamp;
    }

}
