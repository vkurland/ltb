package ltb;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.*;

/**
 * Copyright (c) 2018 Happy Gears
 * author: vadim2
 * Date: 10/21/18
 */
public class FluxFromTest {

    /**
     * Trying to figure out what happens when Flux.fromIterable() is called with an empty iterable.
     * Does it return an empty Flux or times out?
     */
    @Test
    public void test2() throws InterruptedException, ExecutionException, TimeoutException {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            CompletableFuture<Integer> future = Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .reduce((i1, i2) -> i1 + i2)
                    .toFuture();

            Integer result1 = future.get(10, TimeUnit.SECONDS);
            assertNotNull(result1);
            assertEquals((int)result1, 49995000);

            List<Integer> empty = Collections.emptyList();

            future = Flux.fromIterable(empty)
                    .reduce((i1, i2) -> i1 + i2)
                    .toFuture();

            Integer result2 = future.get(10, TimeUnit.SECONDS);

            // future returns null as the result of an empty Flux, but we do not get timeout
            assertNull(result2);

            // adding onEmpty fallback

            future = Flux.fromIterable(empty)
                    .defaultIfEmpty(12345)
                    .reduce((i1, i2) -> i1 + i2)
                    .toFuture();

            Integer result3 = future.get(10, TimeUnit.SECONDS);

            assertNotNull(result3);
            assertEquals((int)result3, 12345);

        } finally {
            client.shutdown();
        }
    }

    @Test
    public void test3() throws InterruptedException, ExecutionException, TimeoutException {

        Map<Integer, Integer> data = IntStream.range(0, 10).boxed().collect(Collectors.toMap(o -> o, o -> o*o));

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            CompletableFuture<Long> future = Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .map(num -> Optional.ofNullable(data.get(num)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .count()
                    .toFuture();

            Long result1 = future.get(10, TimeUnit.SECONDS);
            assertNotNull(result1);
            assertEquals((long)result1, 10);

            List<Integer> empty = Collections.emptyList();

            future = Flux.fromIterable(empty)
                    .map(num -> Optional.ofNullable(data.get(num)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .count()
                    .toFuture();

            Long result2 = future.get(10, TimeUnit.SECONDS);

            assertNotNull(result2);
            assertEquals((long)result2, 0);

            // adding onEmpty fallback

            future = Flux.fromIterable(empty)
                    .defaultIfEmpty(12345)
                    .map(num -> Optional.ofNullable(data.get(num)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .count()
                    .toFuture();

            Long result3 = future.get(10, TimeUnit.SECONDS);

            assertNotNull(result3);
            assertEquals((long)result3, 0);  // note

            // defaultIfEmpty() in a different place

            future = Flux.fromIterable(empty)
                    .map(num -> Optional.ofNullable(data.get(num)))
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .defaultIfEmpty(12345)
                    .count()
                    .toFuture();

            Long result4 = future.get(10, TimeUnit.SECONDS);

            assertNotNull(result4);
            assertEquals((long)result4, 1);  // note

        } finally {
            client.shutdown();
        }
    }

}
