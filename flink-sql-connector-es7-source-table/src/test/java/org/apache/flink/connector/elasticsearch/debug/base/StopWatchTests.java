package org.apache.flink.connector.elasticsearch.debug.base;

import org.apache.flink.connector.elasticsearch.source.commons.StopWatch;
import org.junit.jupiter.api.Test;

public class StopWatchTests {

    @Test
    public void testSimpleStopWatch() {
        // Create a new StopWatch instance
        StopWatch stopWatch = new StopWatch();

        // Start the stopwatch
        stopWatch.start("t1");

        // Simulate some work with a sleep
        try {
            Thread.sleep(1000); // Sleep for 1 second
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Stop the stopwatch
        stopWatch.stop();

        System.out.println(stopWatch);
    }

    @Test
    public void testNestStopWatch() {
        // Create a new StopWatch instance
        StopWatch stopWatch = new StopWatch();

        // Start the stopwatch
        stopWatch.start("t1");
        stopWatch.start("t1.1");
        stopWatch.start("t1.1.1");
        stopWatch.stop(new MyMetrics());
        stopWatch.start("t1.1.2");
        stopWatch.stop();
        stopWatch.start("t1.2");
        stopWatch.stop();

        // Simulate some work with a sleep
        try {
            Thread.sleep(1000); // Sleep for 1 second
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // Stop the stopwatch
        stopWatch.stop();
        stopWatch.stop();

        System.out.println(stopWatch);
    }

    @Test
    public void testSleepStopWatch() throws InterruptedException {
        // Create a new StopWatch instance
        StopWatch stopWatch = new StopWatch();

        // Start the stopwatch
        stopWatch.start("t1");
        stopWatch.start("t1.1");
        stopWatch.start("t1.1.1");
        Thread.sleep(1000); // Sleep for 1 second
        stopWatch.stop(); // stop t1.1.1
        stopWatch.start("t1.1.2");
        Thread.sleep(1000); // Sleep for 1 second
        stopWatch.stop(); // stop t1.1.2
        stopWatch.stop(); // stop t1.1
        stopWatch.start("t1.2");
        Thread.sleep(1000); // Sleep for 1 second
        stopWatch.stop(); // stop t1.2
        stopWatch.stop(); // stop t1

        System.out.println(stopWatch);
    }

    private static class MyMetrics implements StopWatch.Metrics {

        @Override
        public String[] getHeaderArray() {
            return new String[] {"a", "b", "c"};
        }

        @Override
        public Object[] getValueArray() {
            return new Object[] {1, "2", 3.0};
        }
    }
}
