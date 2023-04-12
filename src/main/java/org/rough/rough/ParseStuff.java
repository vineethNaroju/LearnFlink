package org.rough.rough;

import java.io.BufferedReader;
import java.io.FileReader;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

public class ParseStuff {



    public static void print(Object o) {
        System.out.println(o);
    }

    public static void solve(BufferedReader br, Map<String, LocalDateTime> mp, Set<String> tasks) throws Exception {

    }


    public static void main(String[] args) throws Exception {


        Set<String> startTasks = new TreeSet<>();
        Set<String> endTasks = new TreeSet<>();
        Map<String, LocalDateTime> startMap = new TreeMap<>();
        Map<String, LocalDateTime> endMap = new TreeMap<>();

        BufferedReader bufferedReader = new BufferedReader(new FileReader("/Users/narojv/Documents/LearnFlink/starttime_good"));

        String line = bufferedReader.readLine();
        String []words;

        while(line != null) {

            line = bufferedReader.readLine();

            if(line == null) {
                break;
            }

            words = line.split(" ");

            String t = words[1].replace(',', '.');
            LocalDateTime localDateTime = LocalDateTime.parse(words[0] + "T" + t);

            startMap.put(words[7], localDateTime);
            startTasks.add(words[7]);
        }

        // ----------

        print(startTasks.size() + "|" + startMap.size() + "|" + endMap.size());

        BufferedReader bufferedReader2 = new BufferedReader(new FileReader("/Users/narojv/Documents/LearnFlink/endtime_good"));

        String line2 = bufferedReader2.readLine();
        String []words2;

        while(line2 != null) {

            line2 = bufferedReader2.readLine();

            if(line2 == null) {
                break;
            }

            words2 = line2.split(" ");

            String t = words2[1].replace(',', '.');
            LocalDateTime localDateTime = LocalDateTime.parse(words2[0] + "T" + t);

            String task = words2[7];

            if(startTasks.contains(task)) {
                endTasks.add(task);
                endMap.put(task, localDateTime);
            } else {
                print(task + " is missing from start tasks");
            }
        }

        print(startTasks.size() + "|" + startMap.size() + "|" + endTasks.size() + "|" + endMap.size());



        Iterator<String> itr = endTasks.iterator();


        String task;

        double minM = Double.MAX_VALUE, maxM = Double.MIN_VALUE, totalM = 0, cntM = 0;
        String minTaskM = "na-1", maxTaskM = "na-2";

        double minR = Double.MAX_VALUE, maxR = Double.MIN_VALUE, totalR = 0, cntR = 0;
        String minTaskR = "na-1", maxTaskR = "na-2";

        while(itr.hasNext()) {
            task = itr.next();

            LocalDateTime startDate = startMap.get(task);
            LocalDateTime endDate = endMap.get(task);

            // print(task + "|" + startDate + "|" + endDate);

            long minutes = ChronoUnit.MINUTES.between(startDate, endDate);
            long seconds = ChronoUnit.SECONDS.between(startDate, endDate) + 60 * minutes;
            long millis = ChronoUnit.MILLIS.between(startDate, endDate) + 1000 * seconds;

            String []w = task.split("_");

            if(w[3].equals("m")) {
                ++cntM;

                if (millis < minM) {
                    minM = millis;
                    minTaskM = task;
                }

                if (millis > maxM) {
                    maxM = millis;
                    maxTaskM = task;
                }

                totalM += millis;
            } else {
                ++cntR;

                if (millis < minR) {
                    minR = millis;
                    minTaskR = task;
                }

                if (millis > maxR) {
                    maxR = millis;
                    maxTaskR = task;
                }

                totalR += millis;
            }
        }

        print("mapper:");

        print("minTaskM: " + minTaskM + " millis: " + minM);
        print("maxTaskM: " + maxTaskM + " millis: " + maxM);
        print("avg time M: " + (totalM / cntM));

        print("-----------------------------------");

        print("reducer:");
        print("minTaskR: " + minTaskR + " millis: " + minR);
        print("maxTaskR: " + maxTaskR + " millis: " + maxR);
        print("avg time R: " + (totalR / cntR));



    }
}
