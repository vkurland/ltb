package ltb;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.testng.annotations.Test;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.testng.Assert.*;

/**
 * Copyright (c) 2018 Happy Gears
 * author: vadim2
 * Date: 10/21/18
 */
public class FluxSubscriptionsTest {

    /**
     * Trying to figure out what happens when we subscribe to the same Flux twice
     */
    @Test
    public void test1() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux<Integer> input = Flux.fromStream(IntStream.range(0, 10_000).boxed());

            Flux<Integer> f1 = input
                    .sample(Duration.ofMillis(100))
                    .doOnNext(item -> System.out.println(item));

            Mono<Long> f2 = input
                    .map(item -> item * 2)
                    .count();

            f1.subscribe();
            Long total = f2.block();

            assertNotNull(total);
            assertEquals((long)total, 10_000);

        } finally {
            client.shutdown();
        }
    }

    @Test
    public void test2() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Semaphore semaphore = new Semaphore(1);
            ConnectableFlux<Integer> input = Flux.fromStream(IntStream.range(0, 10_000).boxed()).publish();

            input
                    .sample(Duration.ofMillis(100))
                    .doOnNext(item -> System.out.println(item))
                    .subscribe();

            input
                    .map(item -> item * 2)
                    .count()
                    .subscribe(total -> {
                        assertEquals((long)total, 10_000L);
                        semaphore.release();
                    });

            input.connect();

            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

//            assertNotNull(total);
//            assertEquals((long)total, 10_000);

        } finally {
            client.shutdown();
        }
    }

}
