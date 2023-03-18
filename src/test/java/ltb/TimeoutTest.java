package ltb;

import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.LongAdder;

import static org.testng.Assert.assertEquals;

public class TimeoutTest {

    @Test
    public void test1() {

        LongAdder counter = new LongAdder();
        Mono.just(1)
                .timeout(Duration.ofSeconds(1))
                .doOnNext(x -> counter.increment())
                .block();

        assertEquals(counter.longValue(), 1);
    }

    @Test
    public void test2() {
        LongAdder counter = new LongAdder();
        Mono.create(sink -> {
                    sink.success(1);
                })
                .timeout(Duration.ofSeconds(1))
                .doOnNext(x -> counter.increment())
                .block();

        assertEquals(counter.longValue(), 1);

    }

    @Test(expectedExceptions = RuntimeException.class)
    public void test3() {
        LongAdder counter = new LongAdder();
        Mono.create(sink -> {
                    sink.error(new RuntimeException("exception"));
                })
                .timeout(Duration.ofSeconds(1))
                .doOnNext(x -> counter.increment())
                .block();

        assertEquals(counter.longValue(), 1);

    }

    @Test
    public void test4() {
        LongAdder counter = new LongAdder();
        Mono.create(sink -> {
                    sink.success(); // empty
                })
                .timeout(Duration.ofSeconds(1))
                .doOnNext(x -> counter.increment())
                .block();
        // the mono should complete empty rather than throw TimeoutException
        //
        assertEquals(counter.longValue(), 0);

    }
}

