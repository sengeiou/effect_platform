package com.ali.lz.effect.utils;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class StoppableDaemon implements Runnable {
    private static final Log LOG = LogFactory.getLog(StoppableDaemon.class);

    private static final int STOP_TIMEOUT_IN_SECONDS = 120;

    public static void main(String args[]) throws Exception {
        StoppableDaemon s = new StoppableDaemon() {
            public void prepare() {
                System.out.println("prepare...");
            }

            public void execute() {
                System.out.println("execute...");
                try {
                    Thread.sleep(400);
                } catch (Exception ignored) {
                }
            }

            public void shutdown() {
            }
        };
        new Thread(s).start();
        Thread.sleep(3000);
        s.stop();
        System.out.println("88");
        s.stop();
        System.out.println("888");
    }

    private AtomicBoolean stopRequested = new AtomicBoolean(false);
    private CountDownLatch stoppedSignal = new CountDownLatch(1);

    public void run() {
        prepare();
        while (!stopRequested.get()) {
            try {
                execute();
            } catch (Throwable e) {
                LOG.error("execution error", e);
                break;
            }
        }
        LOG.info("out of loop, shutting down...");
        stoppedSignal.countDown();
    }

    public void start() {
        LOG.info("starting up...");
        Thread t = new Thread(this);
        t.setDaemon(true);
        t.start();
    }

    public void stop() {
        LOG.info("stop pending...");
        stopRequested.set(true);
        try {
            stoppedSignal.await(STOP_TIMEOUT_IN_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        LOG.info("shutdown...");
        shutdown();
        LOG.info("finish stopping...");
    }

    public abstract void prepare();

    public abstract void execute();

    public abstract void shutdown();
}
