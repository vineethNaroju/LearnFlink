package org.rough.decom;

//import sun.misc.Signal;
//import sun.misc.SignalHandler;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

public class SignalExp {

//    public void demo() throws Exception {
//        CountDownLatch latch = new CountDownLatch(1);
//
//        Signal.handle(new Signal("USR2"), new SignalHandler() {
//            @Override
//            public void handle(Signal sig) {
//                System.out.println(sig.getName() + "|" + sig.getNumber());
//            }
//        });
//
//        latch.await();
//    }

    public static void main(String[] args) {
        SignalExp exp = new SignalExp();

//        try {
//            exp.demo();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
    }

//    public void learn() throws Exception {
//        CountDownLatch latch = new CountDownLatch(1);
//
//        String str = "PWR HUP INT QUIT ILL TRAP ABRT EMT FPE KILL BUS SEGV SYS PIPE ALRM TERM URG STOP TSTP CONT CHLD TTIN TTOU IO XCPU XFSZ VTALRM PROF WINCH INFO USR1 USR2";
//
//        String[] arr = str.split(" ");
//
//        Set<String> usedOnes = new HashSet<>();
//
//        for(int i=0; i<arr.length; i++) {
//            try {
//                Signal.handle(new Signal(arr[i]), new SignalHandler() {
//                    @Override
//                    public void handle(Signal sig) {
//                        System.out.println(sig.getName() + "|" + sig.getNumber());
//                    }
//                });
//            } catch (Exception e) {
//                usedOnes.add(arr[i]);
//                e.printStackTrace();
//            }
//        }
//
//        System.out.println("Free ones:");
//
//        for(int i=0; i<arr.length; i++) {
//            if(!usedOnes.contains(arr[i])) {
//                System.out.println(arr[i]);
//            }
//        }
//
//
//        latch.await();
//    }
}



/*
*
Free ones:
HUP
INT
TRAP
ABRT
EMT
BUS
SYS
PIPE
ALRM
TERM
URG
TSTP
CONT
CHLD
TTIN
TTOU
IO
XCPU
XFSZ
VTALRM
PROF
WINCH
INFO
USR2
*
*
*
*
*
*
*
*
*
* */