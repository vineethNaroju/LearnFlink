package org.rough.top_k_tags_in_a_minute;

import jnr.ffi.annotations.In;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.AggregateApplyAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.stream.Collectors;


class GenerateRawTweet {

    String block = "The moon has a synchronous rotation with Earth meaning it takes about the same amount of time to " +
            "complete one orbit around Earth as it does to rotate on its own axis The moon continues to be a subject " +
            "of scientific research and exploration with various robotic missions sent to study its geology and composition";

    String[] words = block.split("");

    Random random = new Random(new Date().getTime());


    RawTweet getNext(long val) {

        int textLength = random.nextInt(words.length);

        StringBuilder sb = new StringBuilder();

        for(int i=0; i<textLength; i++) {
            sb.append(words[random.nextInt(textLength)]);
        }

        String text = sb.toString();


        String tweetId = "tweet-" + (random.nextDouble()  * 1e6) + "-val-" + val;

        return new RawTweet(val, tweetId, text);
    }



}


public class TopKTagsInMinute {

    static final GenerateRawTweet generateRawTweet = new GenerateRawTweet();


    public static void main(String[] args) throws Exception {
        final Configuration envConfig = new Configuration();

        envConfig.set(StreamFormat.FETCH_IO_SIZE, MemorySize.ofMebiBytes(10L));

        final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment(envConfig);

        streamEnv.disableOperatorChaining();
        streamEnv.enableCheckpointing(120000); // every 2 minutes

        /*
        *
        * RawTweet(timestamp, tweetId, text)
        * -> partitioned by tweetId hashcode % 10
        * RawTweet(timestamp, tweetId, text) -> split text and produces bunch of tag events per word TagEvent(timestamp, tag, count)
        * -> partitioned by tag TagEvent(timestamp, tag, count)
        * -> sliding window of 1 minute that moves ahead by every 10 seconds.
        * -> perform summation of things in above window
        * -> print to screen (maybe?).
        * */

        DataGeneratorSource<RawTweet> rawTweetGenerator = new DataGeneratorSource<RawTweet>(
                new GeneratorFunction<Long, RawTweet>() {
                    @Override
                    public RawTweet map(Long aLong) throws Exception {
                        return generateRawTweet.getNext(aLong);
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(10),
                TypeInformation.of(RawTweet.class)
        );

        DataStreamSource<RawTweet> streamSource = streamEnv.fromSource(
                rawTweetGenerator,
                WatermarkStrategy.forMonotonousTimestamps(),
                "Raw-Tweet-Stream-Source"
        );

        DataStream<RawTweet> streamKeyedByTweetId = streamSource.keyBy(new KeySelector<RawTweet, Integer>() {
            @Override
            public Integer getKey(RawTweet rawTweet) throws Exception {
                return (rawTweet.tweetId.hashCode() % 10);
            }
        });

        DataStream<TagEvent> tagStream = streamKeyedByTweetId.flatMap(new FlatMapFunction<RawTweet, TagEvent>() {
            @Override
            public void flatMap(RawTweet rawTweet, Collector<TagEvent> collector) throws Exception {

                Map<String, Integer> freq = new HashMap<>();

                String[] arr = rawTweet.text.split("");

                for(String str : arr) {
                    freq.computeIfPresent(str.toLowerCase(), (k, v) -> 1 + v);
                    freq.computeIfAbsent(str.toLowerCase(), (k) -> 1);
                }

                for(Map.Entry<String, Integer> entry : freq.entrySet()) {
                    collector.collect(new TagEvent(rawTweet.timestamp, entry.getKey(), entry.getValue()));
                }
            }
        }).keyBy(new KeySelector<TagEvent, String>() {
            @Override
            public String getKey(TagEvent tagEvent) throws Exception {
                return tagEvent.tag;
            }
        });


        SingleOutputStreamOperator<List<TagFrequency>> aggregatedStream = tagStream
                .windowAll(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                .aggregate(new AggregateFunction<TagEvent, Map<String, Integer> , List<TagFrequency>>() {
                        @Override
                        public Map<String, Integer> createAccumulator() {
                            return new HashMap<>();
                        }

                        @Override
                        public Map<String, Integer> add(TagEvent tagEvent, Map<String, Integer> acc) {
                            acc.computeIfAbsent(tagEvent.tag, (k) -> tagEvent.count);
                            acc.computeIfPresent(tagEvent.tag, (k, v) -> tagEvent.count + v);
                            return acc;
                        }

                        @Override
                        public List<TagFrequency> getResult(Map<String, Integer> acc) {
                            return acc.entrySet().stream()
                                    .map(entry -> new TagFrequency(entry.getKey(), entry.getValue()))
                                    .collect(Collectors.toCollection(ArrayList::new));
                        }

                        @Override
                        public Map<String, Integer> merge(Map<String, Integer> a, Map<String, Integer> b) {

                            for(Map.Entry<String, Integer> entry : b.entrySet()) {
                                a.computeIfAbsent(entry.getKey(), k -> entry.getValue());
                                a.computeIfPresent(entry.getKey(), (k, v) -> entry.getValue() + v);
                            }

                            return a;
                        }
                });



        aggregatedStream.setBufferTimeout(1000)
                .addSink(new SinkFunction<List<TagFrequency>>() {
            @Override
            public void invoke(List<TagFrequency> value, Context context) throws Exception {
               for(TagFrequency tf : value) {
                   System.out.println(new Date() + " ======== " + tf);
               }
            }

            @Override
            public void writeWatermark(Watermark watermark) throws Exception {
                SinkFunction.super.writeWatermark(watermark);
            }

            @Override
            public void finish() throws Exception {
                SinkFunction.super.finish();
            }
        }).name("the-end");

//        aggregatedStream.addSink(new PrintSinkFunction<>(false));

//        aggregatedStream.process(new ProcessFunction<List<TagFrequency>, String>() {
//            @Override
//            public void processElement(List<TagFrequency> tagFrequencies, ProcessFunction<List<TagFrequency>, String>.Context context, Collector<String> collector) throws Exception {
//
//                StringBuilder sb = new StringBuilder(new Date().toString() + " ===== ");
//
//                for(TagFrequency tf : tagFrequencies) sb.append(tf.toString() + " === ");
//
//                sb.append("\n");
//
//                collector.collect(sb.toString());
//            }
//        }).print().name("the-end");

//        aggregatedStream.printToErr();


        streamEnv.execute("top-k-tags-in-minute");
    }
}
