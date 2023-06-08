package org.rough.decom;

import org.apache.flink.util.concurrent.FutureUtils;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class Message {
    String content;
    int id;

    Message(int id, String content) {
        this.content = content;
        this.id = id;
    }

    public String toString() {
        return String.format("Message[id:%d,content:%s]", id, content);
    }
}

public class Fheap {

    public void print(Object o) {
        System.out.println(new Date() + "|" + o);
    }

    public static void main(String[] args) {
        try {
            Fheap feap = new Fheap();
            feap.exp();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public CompletableFuture<Message> trigger(int id, int timeOutSeconds) {
        final CompletableFuture<Message> cf = new CompletableFuture<>();

        new Thread(() -> {
            try {
                Thread.sleep(timeOutSeconds * 1_000);
                cf.complete(new Message( id, "timeOutSeconds=" + timeOutSeconds));
            } catch (Exception e) {
                cf.completeExceptionally(e);
            }
        }).start();

        return cf;
    }

    public void demo() throws Exception {

        CountDownLatch latch = new CountDownLatch(1);
        int jobCount = 5;
        AtomicInteger done = new AtomicInteger(jobCount);
        Random random = new Random();


        for(int i=0; i<jobCount; i++) {

            CompletableFuture<Message> cf = trigger(i, 5 + random.nextInt(10));

            cf.handleAsync((message, throwable) -> {
                print("got " + message);
                if (done.decrementAndGet() <= 0) {
                    print("stop by " +  message);
                    latch.countDown();
                }

               return message;
            });
        }

        print("before latch await");
        latch.await();
        print("after latch await");
    }

    public void exp() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        CompletableFuture<Message> mcf = trigger(1, 10);

        print("now");
        print(mcf.get());

//        mcf.handleAsync((message, throwable) -> {
//            print("entered handle async");
//
//            //Thread.dumpStack();
//            return message;
//        });


        print("before latch await");
        latch.await();
        print("after latch await");
    }


    public void clean() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        int jobCount = 5;
        AtomicInteger done = new AtomicInteger(jobCount);
        Random random = new Random();


        Collection<CompletableFuture<Message>> list = new LinkedList<>();


        for(int i=0; i<jobCount; i++) {

            CompletableFuture<Message> cf = trigger(i, 5 + random.nextInt(10));

            list.add(FutureUtils.orTimeout(cf, 8, TimeUnit.SECONDS));

            cf.handleAsync((message, throwable) -> {
                print("got " + message);
                if (done.decrementAndGet() <= 0) {
                    print("stop by " +  message);
                    latch.countDown();
                }
                return message;
            });
        }


        FutureUtils.waitForAll(list).handleAsync((ack, thr) -> {
            print("stopping as done");
            return ack;
        });

        print("before latch await");
        latch.await();
        print("after latch await");

    }
}
