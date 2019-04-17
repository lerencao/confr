package org.confr.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A base class with some helper variables and methods
 */
public abstract class AbstractServerThread implements Runnable {
    private final CountDownLatch startupLatch;
    private final CountDownLatch shutdownLatch;
    private final AtomicBoolean alive;
    protected Logger logger = LoggerFactory.getLogger(getClass());

    public AbstractServerThread() throws IOException {
        startupLatch = new CountDownLatch(1);
        shutdownLatch = new CountDownLatch(1);
        alive = new AtomicBoolean(false);
    }

    /**
     * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
     */
    public void shutdown() throws InterruptedException {
        alive.set(false);
        shutdownLatch.await();
    }

    /**
     * Wait for the thread to completely start up
     */
    public void awaitStartup() throws InterruptedException {
        startupLatch.await();
    }

    /**
     * Record that the thread startup is complete
     */
    protected void startupComplete() {
        alive.set(true);
        startupLatch.countDown();
    }

    /**
     * Record that the thread shutdown is complete
     */
    protected void shutdownComplete() {
        shutdownLatch.countDown();
    }

    /**
     * Is the server still running?
     */
    protected boolean isRunning() {
        return alive.get();
    }
}
