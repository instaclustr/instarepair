package com.instaclustr.instarepair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Shared sleep and shutdown signals for group of tasks.
 */
public class TaskCoordinator {
    private static final Logger logger = LoggerFactory.getLogger(TaskCoordinator.class);

    /**
     * Is repair running. Used to stop the repair cleanly.
     */
    private volatile boolean isRunning = true;

    /**
     * Lock used to wait for shutdown.
     */
    private Semaphore shutdownLock = new Semaphore(0);

    /**
     * Lock used to sleep.
     */
    private Semaphore sleepLock = new Semaphore(0);

    /**
     * An interruptable sleep.
     *
     * @param timeout Amount of time to wait
     * @param unit    the time unit of the {@code timeout} argument
     */
    public void sleep(long timeout, final TimeUnit unit) {
        try {
            sleepLock.tryAcquire(timeout, unit);
        } catch (InterruptedException e) {
            logger.debug("Sleep interrupted");
        }
    }

    /**
     * Stop tasks.
     */
    public void shutdown() {
        isRunning = false;
        sleepLock.release();
        try {
            shutdownLock.acquire();
        } catch (InterruptedException e) {
            logger.debug("Shutdown interrupted");
        }
    }

    public void abort() {
        isRunning = false;
        sleepLock.release();
    }

    public void shutdownDone() {
        shutdownLock.release();;
    }

    public boolean isRunning() {
        return isRunning;
    }
}
