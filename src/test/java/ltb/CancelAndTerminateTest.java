package ltb;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;

/**
 * Copyright (c) 2019 Happy Gears
 * author: vadim
 * Date: 10/01/19
 */
public class CancelAndTerminateTest {

    /**
     * this calls doOnTerminate() in the downstream Mono. In the end, the counter has value 0 because doOnTerminate()
     * is called every time
     */
    @Test
    public void testNormalTermination1() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));
        AtomicInteger counter = new AtomicInteger(0);

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 5).boxed())
                    .flatMap(num -> useTerminate(commands, counter), 1)
                    .blockLast(Duration.ofSeconds(20));

        } finally {
            client.shutdown();
        }

        assertEquals(counter.get(), 0);
    }

    /**
     * this calls doOnCancel() in the downstream Mono. In the end, the counter has value 5 because
     * doOnCancel() is never called
     */
    @Test
    public void testNormalTermination2() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));
        AtomicInteger counter = new AtomicInteger(0);

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 5).boxed())
                    .flatMap(num -> useCancel(commands, counter), 1)
                    .blockLast(Duration.ofSeconds(20));

        } finally {
            client.shutdown();
        }

        assertEquals(counter.get(), 5); // expect 5 because doOnCancel is never called
    }

    /**
     * this calls both doOnTerminate() and doOnCancel() in the downstream Mono. In the end, the counter has value
     * 0 because doOnTerminate() is called every time
     */
    @Test
    public void testNormalTermination3() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));
        AtomicInteger counter = new AtomicInteger(0);

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 5).boxed())
                    .flatMap(num -> useBoth(commands, counter), 1)
                    .blockLast(Duration.ofSeconds(20));

        } finally {
            client.shutdown();
        }

        assertEquals(counter.get(), 0);
    }

    /**
     * this calls doOnTerminate() in the downstream Mono. The test succeeds (i.e. doOnTerminate()
     * is called every time)
     */
    @Test
    public void testTimeout1() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));
        AtomicInteger counter = new AtomicInteger(0);

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 5).boxed())
                    .flatMap(num -> useTerminate(commands, counter), 1)
                    .blockLast(Duration.ofMillis(2100));

        } catch (final Exception e) {
            // timeout
        } finally {
            client.shutdown();
        }

        // doOnTerminate is called, however, since we spend most of the time inside of the inner Flux,
        // the timeout comes when we are waiting and so the counter is not decremented on the last loop
        assertEquals(counter.get(), 1);

    }

    @Test
    public void testTimeout2() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));
        AtomicInteger counter = new AtomicInteger(0);

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 5).boxed())
                    .flatMap(num -> useCancel(commands, counter), 1)
                    .blockLast(Duration.ofMillis(2100));

        } catch (final Exception e) {
            // timeout
        } finally {
            client.shutdown();
        }

        // doOnCancel is never called and the inner Flux is called 2 times before the timeout occurs
        assertEquals(counter.get(), 2);
    }

    @Test
    public void testTimeout3() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));
        AtomicInteger counter = new AtomicInteger(0);

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 5).boxed())
                    .flatMap(num -> useBoth(commands, counter), 1)
                    .blockLast(Duration.ofMillis(2100));

        } catch (final Exception e) {
            // timeout
        } finally {
            client.shutdown();
        }

        assertEquals(counter.get(), 0);
    }

    private Mono<List<Long>> useTerminate(final RedisReactiveCommands<String, String> commands, final AtomicInteger counter) {
        String key = "CancelAndTerminateTest.set";
        counter.incrementAndGet();
        return Flux.interval(Duration.ofMillis(10))
                .take(100)
                .flatMap(l -> commands.sadd(key, String.valueOf(l)))
                .collectList()
                .doOnTerminate(counter::decrementAndGet);
    }

    private Mono<List<Long>> useCancel(final RedisReactiveCommands<String, String> commands, final AtomicInteger counter) {
        String key = "CancelAndTerminateTest.set";
        counter.incrementAndGet();
        return Flux.interval(Duration.ofMillis(10))
                .take(100)
                .flatMap(l -> commands.sadd(key, String.valueOf(l)))
                .collectList()
                .doOnCancel(counter::decrementAndGet);
    }

    private Mono<List<Long>> useBoth(final RedisReactiveCommands<String, String> commands, final AtomicInteger counter) {
        String key = "CancelAndTerminateTest.set";
        counter.incrementAndGet();
        return Flux.interval(Duration.ofMillis(10))
                .take(100)
                .flatMap(l -> commands.sadd(key, String.valueOf(l)))
                .collectList()
                .doOnCancel(counter::decrementAndGet)
                .doOnTerminate(counter::decrementAndGet);
    }

}
