package org.rough.top_k_tags_in_a_minute;

public class TagFrequency {

    final String tag;
    final int count;

    public TagFrequency(String tag, int count) {
        this.tag = tag;
        this.count = count;
    }

    public String toString() {
        return "<" + tag + "," + count + ">";
    }
}
