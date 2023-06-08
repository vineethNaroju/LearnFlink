package org.rough.decom;


import org.apache.flink.util.concurrent.FutureUtils;

import java.util.Collection;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

class Done {

}

class JM {
    int id;

    public JM(int id) {
        this.id = id;
    }

    void print(Object o) {
        System.out.println( "Job Manager "+ id + "|" + new Date() + "|thread[" + Thread.currentThread().getId() + "-" + Thread.currentThread().getName() + "]|" + o);
    }

    CompletableFuture<Done> triggerCheckpoint() {
        CompletableFuture<Done> done = new CompletableFuture<>();

        new Thread(() -> {
            try {
                print("JM id[" + id + "] is going to sleep");
                Thread.sleep(id * 1_000);
                print("JM id[" + id + "] woke up");
                done.complete(new Done());
                print("JM id[" + id + "] signalled done");
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }).start();

        return done;
    }

    CompletableFuture<Void> decomTM() {

//        CompletableFuture.supplyAsync()

        return CompletableFuture.runAsync(() -> {
            CompletableFuture<Done> done = triggerCheckpoint();

            try {
                done.get();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }
}

class TM {

    void print(Object o) {
        System.out.println( "Task Manager |" + new Date() + "|thread[" + Thread.currentThread().getId() + "-" + Thread.currentThread().getName() + "]|" + o);
    }

    void onStop() {
        print("inside on stop");
    }

    void decom(List<JM> jmList) {
        AtomicInteger cnt = new AtomicInteger(jmList.size());
        Collection<CompletableFuture<Void>> decomFutureCollection = new LinkedList<>();

        for(JM jm : jmList) {
            CompletableFuture<Void> decomFuture = jm.decomTM();
            decomFuture.handleAsync((ack, thrw) -> {

                print("cnt:" + cnt.get());


                if(cnt.decrementAndGet() == 0) {
                    onStop();
                }
                return ack;
            });

            decomFutureCollection.add(decomFuture);
        }

        try {
            FutureUtils.waitForAll(decomFutureCollection).handleAsync((ack, thrw) -> {
                print("inside handlAsync of FutureUtils");
                onStop();
                return ack;
            });
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            print("inside finally");
            onStop();
        }
    }

    void decom2(List<JM> jmList, int timeout) {
        AtomicInteger cnt = new AtomicInteger(jmList.size());
        CompletableFuture<Date> doneCountFuture = new CompletableFuture<>();

        for(JM jm : jmList) {
            CompletableFuture<Void> decomFuture = jm.decomTM();
            decomFuture.handleAsync((ack, thrw) -> {
                if(cnt.decrementAndGet() == 0) {
                    doneCountFuture.complete(new Date());
                }
                return ack;
            });
        }

        try {
            Date completedDate = doneCountFuture.get(timeout, TimeUnit.SECONDS);
            //Date completedDate = FutureUtils.orTimeout(doneCountFuture, timeout, TimeUnit.SECONDS).get();
            print(completedDate);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if(cnt.get() > 0) {
                onStop();
            } else {
                print("No need of onStop()");
            }
        }
    }

    void decom3(List<JM> jmList, int timeout) {

        Collection<CompletableFuture<Void>> decomFutureCollection = new LinkedList<>();

        for(JM jm : jmList) {
            CompletableFuture<Void> decomFuture = jm.decomTM();
            decomFutureCollection.add(decomFuture);
        }

        try {
            FutureUtils.waitForAll(decomFutureCollection).get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            onStop();
        }
    }
}

public class JMTM {

    public static void main(String[] args) {
        JMTM jt = new JMTM();
        jt.demo();
    }

    void demo() {
        List<JM> jmList = new LinkedList<>();

        jmList.add(new JM(3));
        jmList.add(new JM(7));
        jmList.add(new JM(11));
        jmList.add(new JM(15));

        TM tm = new TM();

//        tm.decom(jmList);
//        tm.decom2(jmList, 20);
        tm.decom3(jmList, 9);

        CountDownLatch latch = new CountDownLatch(1);

        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
