package org.rough.datagen;

import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;

import java.util.Random;

public class Entry {
    public static void main(String[] args) {

        RandomGenerator<Integer> randomGenerator = new RandomGenerator<Integer>() {
            private final Random rand = new Random();

            @Override
            public Integer next() {
                return rand.nextInt(1000);
            }
        };


        DataGeneratorSource<Integer> source = new DataGeneratorSource<>(randomGenerator, 10,10000L);

        /**

         aah .... this source also uses rich parallel fn which doesn't have busy time enabled

        **/


    }
}
