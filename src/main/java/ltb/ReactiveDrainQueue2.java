/*
 * Copyright (c) 2019 Happy Gears, Inc.
 * author: colin
 * Date: 9/10/2019
 *
 */
package ltb;

import org.apache.log4j.Logger;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.extra.processor.WorkQueueProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * This is a version of {@link ReactiveDrainQueue} without Redis connections.
 *
 * ReactiveDrainQueue attempts to package up the best method for having multiple long-running threads
 * working together to perform Redis storage operations. Each worker receives its own connection. This
 * pattern should be used when there's no interest in the return value of the Redis operation, and
 * fast async methods should be used to dispose of the data. For cases where the return value is useful,
 * flatMap is preferred.
 *
 * Currently you supply a function which takes a connection and an item. This might be too much freedom,
 * since we want to have some control over the circumstances where this pattern is ued.
 */
@SuppressWarnings("SubscriberImplementation")
public class ReactiveDrainQueue2<T> implements Subscriber<T> {
    private static final Logger log = Logger.getLogger(new Throwable().getStackTrace()[0].getClassName());

    private final WorkQueueProcessor<T> workQueue;
    private final List<Disposable> subscriptions = new ArrayList<>();
    private final Consumer<T> operation;
    private final AtomicLong errorCount = new AtomicLong(0);
    private final int nWorkers;
    private final AtomicBoolean verbose = new AtomicBoolean(true);

    public ReactiveDrainQueue2(final int nWorkers, final Consumer<T> operation) {
        this.nWorkers = nWorkers;
        this.operation = operation;

        workQueue = WorkQueueProcessor.<T>builder()
                .share(true)        // this allows multiple threads to call onNext()
                .bufferSize(1 << 12)  // must be 2^k; how many requests to buffer
                .build();
    }

    public ReactiveDrainQueue2(final int nWorkers,
                               final Consumer<T> operation,
                               final int bufferSize) {
        this.nWorkers = nWorkers;
        this.operation = operation;

        workQueue = WorkQueueProcessor.<T>builder()
                .share(true)        // this allows multiple threads to call onNext()
                .bufferSize(bufferSize)
                .build();
    }

    public void setVerbose(final boolean f) {
        verbose.set(f);
    }

    public void start() {
        for (int i = 0; i < nWorkers; ++i) {
            // final int index = i;  // used for closure below
            subscriptions.add(
                    workQueue.subscribe(
                            item -> {
                                try {
                                    operation.accept(item);
                                } catch (final Throwable t) {
                                    errorCount.incrementAndGet();
                                    if (verbose.get()) {
                                        log.error("ReactiveDrainQueue: ", t);
                                    }
                                }
                            }
                    )
            );
        }
    }

    public void stop() {
        workQueue.shutdown();
        subscriptions.forEach(Disposable::dispose);
    }

    public long getPending() {
        // TODO(colin): the getPending calculation for WorkQueueProcessor is incorrect
        // See: https://github.com/reactor/reactor-core/issues/1889
        // When this is fixed, we should write this as workQueue.getPending();
        return workQueue.getBufferSize() - workQueue.getAvailableCapacity();
    }

    long getErrorCount() {
        return errorCount.get();
    }

    public long getAvailableCapacity() {
        return workQueue.getAvailableCapacity();
    }

    // We implement subscriber via the work queue, so that one of these can be
    // inserted in the subscribe position of an upstream reactive chain.
    @Override
    public void onSubscribe(final Subscription subscription) {
        workQueue.onSubscribe(subscription);
    }

    @Override
    public void onNext(final T t) {
        workQueue.onNext(t);
    }

    @Override
    public void onError(final Throwable throwable) {
        workQueue.onError(throwable);
    }

    @Override
    public void onComplete() {
        workQueue.onComplete();
    }
}
