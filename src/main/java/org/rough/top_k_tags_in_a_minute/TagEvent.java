package org.rough.top_k_tags_in_a_minute;

public class TagEvent {
    final String tag;
    final int count;
    final long timestamp;

    public TagEvent(long timestamp, String tag, int count) {
        this.tag = tag;
        this.count = count;
        this.timestamp = timestamp;
    }
}
