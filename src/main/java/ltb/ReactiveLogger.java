/*
 * Copyright (c) 2019 Happy Gears, Inc.
 * author: colin
 * Date: 3/20/2019
 *
 */
package ltb;

import com.google.common.base.Stopwatch;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Operators;
import reactor.util.context.Context;

import java.time.Duration;
import java.time.Instant;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * ReactiveLogger is meant to sit somewhere in a reactive processing chain, where
 * it will collect statistics without disturbing the stream.
 * @param <T> the type being observed.
 */
public class ReactiveLogger<T> implements Function<Publisher<T>, Publisher<T>> {
    private final String description;
    private final Consumer<String> logger;

    /**
     * Construct an operator on Publishers. The returned function will wrap a Publisher
     * with logging and statistics gathering. Description can be used to tag the purpose
     * of the reactive pipeline. Upon completion, the logger will receive a summary
     * of the flow's statistics.
     */
    public ReactiveLogger(final String description, final Consumer<String> logger) {
        this.description = description;
        this.logger = logger;
    }

    @Override
    public Publisher<T> apply(final Publisher<T> upstream) {
        // This lambda fully implements Publisher<T>: it is the subscription operator for the downstream observable.
        return downstream -> upstream.subscribe(new Implementation(downstream));
    }

    @SuppressWarnings("SubscriberImplementation")
    private class Implementation implements Subscriber<T>, Subscription {
        private Instant subscriptionTime;
        private long itemCount;
        private boolean ok = true;
        private final Stopwatch stopwatch = Stopwatch.createUnstarted();
        private Duration timeToFirstItem;
        private Duration previousItemElapsedTime;
        private Instant completionTime;
        private final DescriptiveStatistics interArrivalStatsMs = new DescriptiveStatistics();
        private final DescriptiveStatistics downstreamProcessingStatsMs = new DescriptiveStatistics();

        final Subscriber<? super T> downstream;
        Subscription subscription;
        boolean done = false;

        Implementation(final Subscriber<? super T> downstream) {
            this.downstream = downstream;
        }

        @Override
        public void onSubscribe(final Subscription subscription) {
            stopwatch.start();
            this.subscription = subscription;
            subscriptionTime = Instant.now();
            downstream.onSubscribe(this);
        }

        @Override
        public void onNext(final T t) {
            if (done) {
                Operators.onNextDropped(t, Context.empty());
                return;
            }
            final Duration elapsed = stopwatch.elapsed();
            ++itemCount;
            if (timeToFirstItem == null) {
                timeToFirstItem = stopwatch.elapsed();
            } else {
                Duration interArrivalTime = elapsed.minus(previousItemElapsedTime);
                interArrivalStatsMs.addValue(interArrivalTime.toNanos()/1e6);
            }
            previousItemElapsedTime = elapsed;
            downstream.onNext(t);
            Duration downstreamInterval = stopwatch.elapsed().minus(elapsed);
            downstreamProcessingStatsMs.addValue(downstreamInterval.toNanos()/1e6);
        }

        @Override
        public void onError(final Throwable t) {
            if (done) {
                Operators.onErrorDropped(t, Context.empty());
                return;
            }
            completionTime = Instant.now();
            ok = false;
            finish();
            downstream.onError(t);
        }

        @Override
        public void onComplete() {
            done = true;
            completionTime = Instant.now();
            finish();
            downstream.onComplete();
        }

        // While the observer operations travel downstream, the subscription operations travel upstream.
        @Override
        public void cancel() {
            subscription.cancel();
        }

        @Override
        public void request(final long l) {
            subscription.request(l);
        }

        private void finish() {
            stopwatch.stop();
            String inter = "", down = "";
            if (itemCount > 1) {
                inter = String.format("; ttf %.2f inter (min %.2f max %.2f mean %.2f sd %.2f)",
                        timeToFirstItem.toNanos()/1e6,
                        interArrivalStatsMs.getMin(),
                        interArrivalStatsMs.getMax(),
                        interArrivalStatsMs.getMean(),
                        interArrivalStatsMs.getStandardDeviation());
                down = String.format(" downstream (min %.2f max %.2f mean %.2f sd %.2f)",
                        downstreamProcessingStatsMs.getMin(),
                        downstreamProcessingStatsMs.getMax(),
                        downstreamProcessingStatsMs.getMean(),
                        downstreamProcessingStatsMs.getStandardDeviation());
            }
            String s = String.format("%s start %s count %d %s %s (%s)%s%s",
                    description,
                    subscriptionTime,
                    itemCount,
                    ok ? "complete" : "error",
                    completionTime,
                    stopwatch,
                    inter,
                    down);
            logger.accept(s);
        }
    }

    @Override
    public String toString() {
        return String.format("rlog(%s)", description);
    }
}
