package ltb;

import com.google.common.annotations.VisibleForTesting;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * This version of ReactiveQueueAdapter (which we're not using now) is
 * a bare-bones implementation that does everything synchronously in the
 * subscribe hook. It does not create a carry thread of its own; instead
 * it relies on careful management of the threading on the client side.
 */
public class ReactiveQueueAdapter2<T> implements Consumer<FluxSink<T>> {
    private Duration pollDuration = Duration.ofSeconds(1);
    private final BlockingQueue<T> q;
    private volatile boolean done = false;
    private long maxDemand = 0;
    // private final AtomicLong demand = new AtomicLong(0);
    private FluxSink<T> downstream;

    public static <T> Flux<T> of(final BlockingQueue<T> q) {
        ReactiveQueueAdapter<T> rqa = new ReactiveQueueAdapter<>(q);
        return Flux.create(rqa);
    }

    // Prefer ReactiveQueueAdapter.of(q) in production.
    @VisibleForTesting
    ReactiveQueueAdapter2(final BlockingQueue<T> q) {
        this.q = q;
    }

    @VisibleForTesting
    public void accept(final FluxSink<T> sink) {
        downstream = sink;
        sink.onRequest(this::request);
        sink.onDispose(() -> done = true);
    }

    private void request(final long n) {
        if (n > maxDemand) maxDemand = n;
        int delivered = 0;
        while (!done && delivered < n) {
            // If we have an item handy, send it along.
            try {
                final T item = q.poll(pollDuration.get(ChronoUnit.NANOS), TimeUnit.NANOSECONDS);
                if (item != null) {
                    downstream.next(item);
                    ++delivered;
                }
            } catch (InterruptedException e) {
                downstream.error(e);
                return;
            }
        }
    }

    void setPollDuration(final Duration duration) { pollDuration = duration; }
    long getMaxDemand() { return maxDemand; }
    void waitForExit() { }
}

