package ltb;

import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class OnErrorContinueTest {

    @Test
    public void test1() {

        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);

        Long result = Flux.interval(Duration.ofMillis(10))
                .take(20)
                .publishOn(Schedulers.elastic())
                .onBackpressureDrop()
                .doOnNext(l -> counter.incrementAndGet())
                .flatMap(this::nop)
                .onErrorContinue((e, o) -> errors.incrementAndGet())
                .count()
                .block(Duration.ofSeconds(20));

        assertNotNull(result);
        assertEquals(counter.longValue(), 20L);
        assertEquals(errors.longValue(), 0);

    }

    @Test
    public void test2() {

        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);

        Long result = Flux.interval(Duration.ofMillis(10))
                .take(20)
                .publishOn(Schedulers.elastic())
                .onBackpressureDrop()
                .doOnNext(l -> counter.incrementAndGet())
                // call something that can cause error
                .flatMap(this::triggerError)
                .onErrorContinue((e, o) -> errors.incrementAndGet())
                .count()
                .block(Duration.ofSeconds(20));

        assertNotNull(result);
        assertEquals(counter.longValue(), 20L);
        assertEquals(errors.longValue(), 20L);

    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void test3() {

        AtomicInteger counter = new AtomicInteger(0);
        AtomicInteger errors = new AtomicInteger(0);

        Long result = Flux.interval(Duration.ofMillis(10))
                .take(20)
                .publishOn(Schedulers.elastic())
                .onBackpressureDrop()
                // call onErrorContinue() above the place where exception is thrown. It does not help here
                .onErrorContinue((e, o) -> errors.incrementAndGet())
                .doOnNext(l -> counter.incrementAndGet())
                // call something that can cause error
                .flatMap(this::triggerError)
                .count()
                .block(Duration.ofSeconds(20));

        assertNotNull(result);
        assertEquals(counter.longValue(), 20L);
        assertEquals(errors.longValue(), 20L);

    }

    private Mono<Long> triggerError(long l) {
        throw new IllegalArgumentException();
    }

    private Mono<Long> nop(long l) {
        return Mono.just(l);
    }
}
