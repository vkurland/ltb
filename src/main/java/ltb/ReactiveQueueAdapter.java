package ltb;

import com.google.common.annotations.VisibleForTesting;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class ReactiveQueueAdapter<T> implements Consumer<FluxSink<T>> {
    private Duration pollDuration = Duration.ofSeconds(1);
    private final BlockingQueue<T> q;
    private long maxDemand = 0;
    private final AtomicLong demand = new AtomicLong();
    private volatile boolean done = false;
    private final Semaphore exited = new Semaphore(0);
    private final Lock demandLock = new ReentrantLock();
    private final Condition nonZeroDemand = demandLock.newCondition();

    public static <T> Flux<T> of(final BlockingQueue<T> q) {
        ReactiveQueueAdapter<T> rqa = new ReactiveQueueAdapter<>(q);
        return Flux.create(rqa);
    }

    // Prefer ReactiveQueueAdapter.of(q) in production.
    @VisibleForTesting
    ReactiveQueueAdapter(final BlockingQueue<T> q) {
        this.q = q;
    }

    @VisibleForTesting
    public void accept(final FluxSink<T> sink) {
        Thread t = new Thread(() -> carryTo(sink));
        sink.onRequest(requested -> {
            demandLock.lock();
            try {
                System.out.println("RQA demand set to " + requested);
                demand.set(requested);
                if (requested > maxDemand) maxDemand = requested;
                if (requested > 0) nonZeroDemand.signalAll();
            } finally {
                demandLock.unlock();
            }
        });
        sink.onDispose(() -> {
            demandLock.lock();
            try {
                done = true;
                nonZeroDemand.signalAll();
            } finally {
                demandLock.unlock();
            }
        });
        t.setDaemon(true);
        t.start();
    }

    /**
     * Carry unloads items from the queue and pushes them down the reactive chain.
     * It respects the backpressure demand.
     */

    private void carryTo(final FluxSink<T> sink) {
        List<T> items = new ArrayList<>();
        long toSend;
        LOOP: while (!sink.isCancelled()) {
            // We don't want to poll if demand is zero, since polling will retrieve
            // an object. If there is no demand, we would be violating the reactive
            // contract if we supplied it, and if the queue from which we drew the
            // object is finite, we may already have lost the opportunity to push the
            // item back on the queue. So we park here until there is demand.
            demandLock.lock();
            try {
                while ((toSend = demand.get()) <= 0) {
                    try {
                        nonZeroDemand.await();
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break LOOP;
                    }
                    // We can also be awakened if the downstream has unsubscribed.
                    if (done) break LOOP;
                }
            } finally {
                demandLock.unlock();
            }
            try {
                // The interval doesn't matter much here; we want to wake up as soon
                // as there is supply. However, this interval determines how soon we
                // will notice that our subscriber has disconnected. When that happens,
                // we just return from here, allowing the thread to exit.
                final T item1 = q.poll(pollDuration.toMillis(), TimeUnit.MILLISECONDS);
                if (item1 == null) continue;
                items.add(item1);
            } catch (final InterruptedException e) {
                sink.error(e);
                Thread.currentThread().interrupt();
                break;
            }
            // Being sensitive to demand is how to respect backpressure
            if (toSend > Integer.MAX_VALUE) {
                // If backpressure is not active, downstream will request Long.MAX_VALUE
                // items. We react to this by draining all that we have. We do the same
                // if the value exceeds Integer.MAX_VALUE, since drainTo doesn't take a
                // long argument.
                q.drainTo(items);
            } else if (toSend > 1) {
                // Note: we already have one item because of poll(). So we ask for one
                // fewer than the demand here
                q.drainTo(items, (int) toSend - 1);
            }
            demand.addAndGet(-items.size());
            items.forEach(sink::next);
            items.clear();
        }
        exited.release(Integer.MAX_VALUE);
    }

    long getMaxDemand() { return maxDemand; }
    void setPollDuration(final Duration d) { pollDuration = d; }

    @VisibleForTesting
    void waitForExit() { exited.acquireUninterruptibly(); }
}

