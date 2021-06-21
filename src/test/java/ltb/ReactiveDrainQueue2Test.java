/*
 * Copyright (c) 2019 Happy Gears, Inc.
 * author: colin
 * Date: 9/25/2019
 *
 */
package ltb;

import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.*;

@Test(singleThreaded = true)
public class ReactiveDrainQueue2Test {

    /** This test is disabled because it is a reproducing test case for NET-3291.
     * Since the bug has been fixed, the ReactiveDrainQueue does not fail in this
     * way and this test would fail.
     */

    @Test(enabled=false)
    public void testExceptionOperationUNFIXED() throws InterruptedException {
        final AtomicInteger processed = new AtomicInteger(0);
        final AtomicBoolean thrown = new AtomicBoolean(false);
        ReactiveDrainQueue2<Integer> rdq = null;
        try {
            rdq = new ReactiveDrainQueue2<>(1, i -> {
                if (i % 10 == 0) throw new RuntimeException("uh oh");
                processed.incrementAndGet();
            });
            rdq.start();

            for (int i = 1; i < 2001; ++i) {
                rdq.onNext(i);
            }
        } catch (final RuntimeException r) {
            thrown.set(true);
            assertNotNull(rdq);

            // The queue is dead now, the exception having killed the one thread that was serving it.
            Thread.sleep(100);
            assertEquals(rdq.getPending(), 2000-10);
            assertEquals(rdq.getAvailableCapacity(), 4096-1990);
        }
        assertTrue(thrown.get());
        rdq.stop();
    }

    /** See NET-3291 */
    @Test(invocationCount=100, singleThreaded = true)
    public void testExceptionOperation() throws InterruptedException {
        final AtomicInteger processed = new AtomicInteger(0);
        final Semaphore count = new Semaphore(0);
        ReactiveDrainQueue2<Integer> rdq = new ReactiveDrainQueue2<>(1, i -> {
            if (i % 10 == 0) throw new RuntimeException("uh oh");
            processed.incrementAndGet();
            count.release();
        });

        rdq.setVerbose(false); // suppress logging of caught exceptions in worker function
        rdq.start();

        for (int i = 1; i < 2000; ++i) {
            rdq.onNext(i);
        }
        count.acquireUninterruptibly(1800);
        // At the point where the semaphore value becomes 1800, we are sitting at the line `count.release()`
        // within the lambda of RDQ. That means the function has not returned yet when we are rescheduled here;
        // we may still observe (at most) one element pending.
        assertTrue(rdq.getPending() <= 1, "actual rdq.getPending()=" + rdq.getPending());
        assertTrue(rdq.getAvailableCapacity() >= 4095 && rdq.getAvailableCapacity() <= 4096);
        assertTrue(rdq.getErrorCount() >= 199 && rdq.getErrorCount() <= 200);
        assertEquals(processed.get(), 1800);
        rdq.stop();
    }

    @Test
    public void testOverflow1()
    {
        final AtomicInteger processed = new AtomicInteger(0);
        final Semaphore count = new Semaphore(0);
        ReactiveDrainQueue2<Integer> rdq = new ReactiveDrainQueue2<>(1, i -> {
            Mono.delay(Duration.ofMillis(1)).block();
            processed.incrementAndGet();
            count.release();
        });

        rdq.start();

        // internal queue/buffer can hold 4096 items. Lets try to send more. Note that queue processing lambda
        // introduces 1ms delay, so it should take ~8sec to process all items, but this loop tries to
        // send them much faster
        for (int i = 0; i < 10_000; i++) {
            rdq.onNext(i);
        }

        int processedSoFar = processed.get();

        // actually it is 10000-4096= 5904 because we get control back only when all items have been sent and fit
        // in the buffer. But since the adapter continues to process the queue once we get here, the actual number
        // may vary a little. I do not want to rely on precise timing
        assertTrue(5890 < processedSoFar && processedSoFar < 5920, String.valueOf(processedSoFar));

        count.acquireUninterruptibly(10000);

        assertTrue(rdq.getPending() <= 1, String.valueOf(rdq.getPending()));
        assertTrue(rdq.getAvailableCapacity() >= 4095 && rdq.getAvailableCapacity() <= 4096, String.valueOf(rdq.getAvailableCapacity()));
        assertEquals(rdq.getErrorCount(), 0, String.valueOf(rdq.getErrorCount()));
        assertEquals(processed.get(), 10000, String.valueOf(processed.get()));
        rdq.stop();
    }
}
