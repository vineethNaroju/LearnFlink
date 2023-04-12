package org.rough.file;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Random;

public class GenerateData {

    public static void main(String[] args) throws  Exception {

        //String path = args[0];



        int n  = Runtime.getRuntime().availableProcessors();


        ArrayList<Thread> threads = new ArrayList<>();


        for(int k=0; k<n; k++) {
            String fileName = "sma000" + k;

            Thread t = new Thread(() -> {
                try {
                    FileWriter fw = new FileWriter("/Users/narojv/input/" + fileName);

                    Random random = new Random(System.currentTimeMillis());

                    for(int i=0; i<50000; i++) {
                        for(int j=0; j<100; j++) {
                            fw.write((random.nextInt(10000) + " "));
                        }
                        fw.write("\n");
                    }

                    fw.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });

            threads.add(t);
        }


        for(Thread t : threads) {
            t.start();
        }

        for(Thread t : threads) {
            t.join();
        }








    }
}
