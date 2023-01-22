package org.example;

import java.util.ArrayList;
import java.util.Date;

public class ThreadIntro {

    // Look into process memory allocation layout - bss, stack, heap, etc.
    // Threads have their own stack but share heap of a process.
    // In java non-primitive thing is allocated on heap.


    int val = 0;


    void print(Object o) {
        System.out.println(new Date().getTime() + "|" + o);
    }


    public static void main(String[] args) {

        ThreadIntro ti = new ThreadIntro();


        // extend a thread class -> all the functionalities -> heavy
        // implement a runnable interface -> light weight


        int threadCount = 100;

        ArrayList<Thread> threads = new ArrayList<>();


        for(int i=0; i<threadCount; i++) {
            Thread t = new Thread(() -> {

                String threadName = Thread.currentThread().getName();


                try {
                    Thread.sleep(10);
                } catch (Exception e) {

                }

                //ti.print("ti.val:" + ti.val + " as seen by:" + threadName);

                synchronized (ti) {
                    ti.val++; // ti.val = ti.val + 1;
                }


                ti.print("ti.val:" + ti.val + " after inc by:" + threadName);

            }, "thread" + i);

            threads.add(t);
        }

        for(Thread t : threads) {
            t.start();
        }


        for(Thread t : threads) {
            try {
                t.join();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }


        System.out.println("ti.val:" + ti.val + " as seen by:" + Thread.currentThread().getName());


    }
}
