/*
 * Copyright (c) 2019 Happy Gears, Inc.
 * author: colin
 * Date: 3/20/2019
 *
 */
package ltb;

import org.testng.Assert;
import org.testng.annotations.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.util.concurrent.Queues;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.fail;

public class ReactiveQueueAdapterTest {

    @Test
    public void testSimple() {
        BlockingQueue<Integer> q = new LinkedBlockingDeque<>();
        Flux<Integer> f = ReactiveQueueAdapter.of(q);
        Semaphore semaphore = new Semaphore(0);
        Disposable subscription = f.take(10).collectList().subscribe(
                l -> {
                    assertEquals(l, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
                    semaphore.release();
                });
        IntStream.range(1, 20).forEach(q::add);
        semaphore.acquireUninterruptibly();
        subscription.dispose();
    }

    @Test
    public void testQueueWithCapacity() {
        final int N = 200;
        BlockingQueue<Integer> q = new ArrayBlockingQueue<>(100);
        Flux<Integer> f = ReactiveQueueAdapter.of(q);
        Semaphore semaphore = new Semaphore(0);
        Disposable subscription = f.subscribeOn(Schedulers.parallel()).map(i -> 2 * i).subscribe(
                l -> {
                    if (l == 2 * (N - 1)) semaphore.release();
                });
        IntStream.range(1, N).forEach(i -> {
            try {
                // This will soon block, because of the queue capacity, but
                // the subscription will catch up eventually.
                q.put(i);
            } catch (final InterruptedException e) {
                // intentionally discarded
            }
        });
        // Released when the last number has made it to the subscriber.
        semaphore.acquireUninterruptibly();
        subscription.dispose();
    }

    @Test
    public void testAsyncConsumerLimitRate() {
        final int N = 10000;
        BlockingQueue<Integer> q = new LinkedBlockingDeque<>();
        ReactiveQueueAdapter<Integer> rqa = new ReactiveQueueAdapter<>(q);
        Flux<Integer> f = Flux.create(rqa);
        Semaphore semaphore = new Semaphore(0);
        Disposable subscription = f.subscribeOn(Schedulers.parallel()).limitRate(100).take(N).collectList().subscribe(
                l -> {
                    assertEquals(l.size(), N);
                    semaphore.release();
                }
        );
        IntStream.range(0, N).forEach(q::add);
        semaphore.acquireUninterruptibly();
        // We don't have real visibility into the backpressure algorithm, but if
        // backpressure is not being used, then the demand is Long.MAX_VALUE.
        // This test might require adjustment when our version of reactor changes.
        // Or we could test that the max demand was less than Long.MAX_VALUE but
        // that seems too lenient.
        assertTrue(rqa.getMaxDemand() < 200);
        assertTrue(rqa.getMaxDemand() > 1);
        subscription.dispose();
        rqa.waitForExit();
    }

    @Test
    public void testAsyncConsumerEmpty() {
        final int N = 10000;
        BlockingQueue<Integer> q = new LinkedBlockingDeque<>();
        Flux<Integer> f = ReactiveQueueAdapter.of(q);
        Semaphore semaphore = new Semaphore(0);
        Disposable subscription = f.subscribeOn(Schedulers.parallel()).take(N).collectList().subscribe(
                l -> {
                    assertEquals(l.size(), N);
                    semaphore.release();
                }
        );

        // wait 5 sec before adding items to the queue. ReactiveQueueAdapter uses internal interval of 1 sec
        // when it polls the queue, the wait here should be longer to provoke queue.poll() return with null
        try {
            Thread.sleep(5000);

            IntStream.range(0, N).forEach(q::add);
            // this times out if the Flux never emitted anything, so our onNext observer
            // never had a chance to release the semaphore. The timeout should be longer than the sleep time
            // above to ensure items were in fact added to the queue
            assertTrue(semaphore.tryAcquire(10, TimeUnit.SECONDS),
                    "attempt to acquire semaphore timed out, this means Flux never emitted anything");

        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            fail();
        }
        subscription.dispose();
    }

    @Test
    public void threadStopsAfterSubscriptionRetires() {
        BlockingQueue<Integer> q = new LinkedBlockingDeque<>();
        ReactiveQueueAdapter<Integer> rqa = new ReactiveQueueAdapter<>(q);
        Flux<Integer> f = Flux.create(rqa);
        Semaphore semaphore = new Semaphore(0);
        f.subscribeOn(Schedulers.parallel()).take(10).collectList().subscribe(
                l -> {
                    assertEquals(l, Arrays.asList(0, 1, 2, 3, 4, 5, 6, 7, 8, 9));
                    semaphore.release();
                }
        );
        IntStream.range(0, 20).forEach(q::add);
        rqa.waitForExit();
    }

    @Test
    public void threadStopsAfterDispose() {
        BlockingQueue<Integer> q = new LinkedBlockingDeque<>();
        ReactiveQueueAdapter<Integer> rqa = new ReactiveQueueAdapter<>(q);
        Flux<Integer> f = Flux.create(rqa);
        // This subscription will never activate, since collectList can only
        // succeed if the source completes. But we will cancel instead.
        final Disposable subscription = f.subscribeOn(Schedulers.parallel()).collectList().subscribe(
                l -> fail(),
                t -> fail(),
                Assert::fail
        );
        IntStream.range(0, 20).forEach(q::add);
        subscription.dispose();
        rqa.waitForExit();
    }

    @Test(invocationCount = 20, enabled = false)
    public void flowControlTest() {
        BlockingQueue<Integer> q = new LinkedBlockingDeque<>();
        ReactiveQueueAdapter<Integer> rqa = new ReactiveQueueAdapter<>(q);
        rqa.setPollDuration(Duration.ofMillis(20));
        final int N = 100;
        Flux<Integer> f = Flux.create(rqa);
        Semaphore s = new Semaphore(0);
        Semaphore output = new Semaphore(0);
        // The gate converts semaphore permits into reactive events.
        Flux<Integer> gate = Flux.push(sink -> {
            int i = 0;
            while (!sink.isCancelled()) {
                try {
                    if (s.tryAcquire(1, TimeUnit.SECONDS)) sink.next(i++);
                } catch (final InterruptedException e) {
                    return;
                }
            }
        });
        // The subscription trips a semaphore for every element received. In this way,
        // we can measure how much flow has occurred. We use zip to choke the flow
        // using a manual gate, so we can watch the counts evolve at various points.
        Disposable subscription = f.zipWith(gate).subscribeOn(Schedulers.parallel()).subscribe(t -> {
            System.out.println("rls " + t);output.release(); });
        IntStream.range(0, N).forEach(q::add);
        s.release(1);
        output.acquireUninterruptibly(1);
        // At this point, we expect that a demand of Queues.XS_BUFFER_SIZE (32, as of
        // this writing) has been made, and that the RQA has therefore drained that many
        // items from the queue and delivered them downstream. Only one permit was issued,
        // though, so we expect that the remainder have been buffered downstream.
        assertEquals(rqa.getMaxDemand(), Queues.XS_BUFFER_SIZE);
        assertEquals(q.size(), N - Queues.XS_BUFFER_SIZE);
        // We could continue, therefore, to retrieve another element, without provoking
        // any more demand from upstream.
        s.release(1);
        output.acquireUninterruptibly(1);
        // For a while, we can continue to draw items without provoking any demand from
        // upstream. Verify that no more elements have drained from the blocking queue.
        assertEquals(rqa.getMaxDemand(), Queues.XS_BUFFER_SIZE);
        assertEquals(q.size(), N - Queues.XS_BUFFER_SIZE);
        // Now just drain all the remaining elements.
        s.release(N-2);
        output.acquireUninterruptibly(N-2);
        subscription.dispose();
        rqa.waitForExit();
    }

    @Test(invocationCount = 10)
    public void queueAdaptersAreHotSources() {
        BlockingQueue<Integer> q = new LinkedBlockingDeque<>();
        IntStream.range(1, 4).forEach(q::add);
        // We publish/refCount because we want one driver-thread to service
        // multiple subscriptions, instead of one driver per subscription (in
        // that case, the threads would compete to drain elements from the
        // source queue
        Flux<Integer> f = ReactiveQueueAdapter.of(q)
                .subscribeOn(Schedulers.newSingle("test"))
                .publish()
                .refCount();
        List<Integer> observations = new ArrayList<>();
        Semaphore s = new Semaphore(0);
        Consumer<Integer> reactor = i -> { observations.add(i); s.release(); };
        Disposable d1 = f.subscribe(reactor);
        s.acquireUninterruptibly(3);
        assertEquals(observations, Arrays.asList(1, 2, 3));
        // Now add a new subscription and push a few more items into the queue.
        // With two subscriptions active, we should see double recording.
        Disposable d2 = f.subscribe(reactor);
        IntStream.range(4, 7).forEach(q::add);
        s.acquireUninterruptibly(6);
        assertEquals(observations, Arrays.asList(1, 2, 3, 4, 4, 5, 5, 6, 6));
        // Now discard the extra subscription; entries no longer doubled
        d2.dispose();
        IntStream.range(7, 10).forEach(q::add);
        s.acquireUninterruptibly(3);
        assertEquals(observations, Arrays.asList(1, 2, 3, 4, 4, 5, 5, 6, 6, 7, 8, 9));
        d1.dispose();
    }

    @Test(invocationCount = 5)
    public void groupByTest() {
        BlockingQueue<Integer> q = new LinkedBlockingDeque<>();
        Flux<Integer> f = ReactiveQueueAdapter.of(q);
        Semaphore semaphore = new Semaphore(0);
        List<List<Integer>> results = new ArrayList<>();
        final int sampleSize = 10;
        final Disposable subscription = f
                .subscribeOn(Schedulers.newSingle("test-subscription"))
                .groupBy(i -> i % 3)
                .flatMap(g -> g.buffer(sampleSize))
                .take(10)
                .subscribe(results::add, e -> fail(), semaphore::release);
        Random random = new Random();
        IntStream.generate(() -> random.nextInt(1000)).limit(1000).forEach(q::add);
        semaphore.acquireUninterruptibly();
        results.forEach(r -> {
            assertEquals(r.size(), sampleSize);
            final int j0mod3 = r.get(0) % 3;
            assertTrue(r.stream().allMatch(j -> (j % 3) == j0mod3));
        });
        subscription.dispose();
    }
}
