package com.demobank.repository;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.Random;
import java.util.concurrent.Callable;

import static org.junit.Assert.fail;

public class TestUtils {
    private static Random random = new Random();
    public static File tempDir(String prefix) {
        var ioDir = System.getProperty("java.io.tmpdir");
        var f = new File(ioDir, prefix + random.nextInt(1000000));
        f.mkdirs();
        f.deleteOnExit();
        return f;
    }

    public static void waitUntilTrue(Callable<Boolean> predicate, String msg,
                                     Duration waitTime) {
        try {
            var startTime = System.nanoTime();
            while (true) {
                if (predicate.call())
                    return;

                if (System.nanoTime() > (startTime + waitTime.toNanos())) {
                    fail(msg);
                }

                Thread.sleep(100);
            }
        } catch (Exception e) {
            // should never hit here
            throw new RuntimeException(e);
        }
    }
}
