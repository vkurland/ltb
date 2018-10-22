package ltb;

import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Copyright (c) 2018 Happy Gears
 * author: vadim2
 * Date: 10/21/18
 */
public class LettuceTest {

    private final String sourceKey = "test.source";
    private final String targetKey = "test.target";

    /**
     * create source set of 10000 elements, then build Flux that reads these elements and for each one
     * creates 4 new elements that it writes to the target set.
     *
     * The target set should end up with 10000*4=40000 elements
     *
     */
    @Test
    public void test2() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.sadd(sourceKey, String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceCard = commands.scard(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceCard);
            assertEquals(sourceCard.longValue(), 10_000L);

            commands.smembers(sourceKey)
                    .flatMap(num -> Flux.fromStream(IntStream.range(0, 4).boxed()).map(sfx -> num + "-" + sfx))
                    .flatMap(item -> commands.sadd(targetKey, item))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 40_000L);

        } finally {
            client.shutdown();
        }
    }

    /**
     * create hash with 10000 elements, then read it and for each element create 4 new elements and
     * write them to the target set.
     *
     * The target set should end up with 10000*4=40000 elements
     *
     * This test FAILS
     *
     */
    @Test
    public void test3() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.hset(sourceKey, String.valueOf(num), String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceSize = commands.hlen(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceSize);
            assertEquals(sourceSize.longValue(), 10_000L);

            ScanStream.hscan(commands, sourceKey)
                    .filter(Value::hasValue)
                    .map(Value::getValue)
                    .flatMap(num -> Flux.fromStream(IntStream.range(0, 4).boxed()).map(sfx -> num + "-" + sfx))
                    .flatMap(item -> commands.sadd(targetKey, item))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 40_000L);

        } finally {
            client.shutdown();
        }
    }

    /**
     * simplified version of test3 that just copies elements from source to target using one flatMap() call
     *
     * create hash with 10000 elements, then read it and write each element to the target set.
     *
     * The target set should end up with 10000 elements
     *
     * This test PASSES
     */
    @Test
    public void test4() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.hset(sourceKey, String.valueOf(num), String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceSize = commands.hlen(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceSize);
            assertEquals(sourceSize.longValue(), 10_000L);

            ScanStream.hscan(commands, sourceKey)
                    .filter(Value::hasValue)
                    .map(Value::getValue)
                    .flatMap(item -> commands.sadd(targetKey, item))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 10_000L);

        } finally {
            client.shutdown();
        }

    }

    /**
     * create hash with 10000 elements, then read it and for each element create 4 new elements and
     * write them to the target set.
     *
     * The target set should end up with 10000*4=40000 elements
     *
     * Unlike test3, this uses concatMap() with the first Flux
     *
     * This test FAILS
     */
    @Test
    public void test5() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.hset(sourceKey, String.valueOf(num), String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceSize = commands.hlen(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceSize);
            assertEquals(sourceSize.longValue(), 10_000L);

            ScanStream.hscan(commands, sourceKey)
                    .filter(Value::hasValue)
                    .map(Value::getValue)
                    .concatMap(num -> Flux.fromStream(IntStream.range(0, 4).boxed()).map(sfx -> num + "-" + sfx))
                    .flatMap(item -> commands.sadd(targetKey, item))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 40_000L);

        } finally {
            client.shutdown();
        }
    }

    /**
     * create hash with 10000 elements, then read it and for each element create 4 new elements and
     * write them to the target set.
     *
     * The target set should end up with 10000*4=40000 elements
     *
     * this uses concatMap() to merge the Flux that creates "expanded" elements, as well as to
     * merge the Flux that writes them to Redis.
     *
     * THIS WORKS
     *
     */
    @Test
    public void test6() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.hset(sourceKey, String.valueOf(num), String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceSize = commands.hlen(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceSize);
            assertEquals(sourceSize.longValue(), 10_000L);

            ScanStream.hscan(commands, sourceKey)
                    .filter(Value::hasValue)
                    .map(Value::getValue)
                    .concatMap(num -> Flux.fromStream(IntStream.range(0, 4).boxed()).map(sfx -> num + "-" + sfx))
                    .concatMap(item -> commands.sadd(targetKey, item))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 40_000L);

        } finally {
            client.shutdown();
        }
    }

    /**
     * create hash with 10000 elements, then read it and for each element create 4 new elements and
     * write them to the target set.
     *
     * The target set should end up with 10000*4=40000 elements
     *
     * this uses flatMap() to merge the Flux that creates "expanded" elements and concatMap() to
     * merge the Flux that writes them to Redis.
     *
     * THIS FAILS
     *
     */
    @Test
    public void test7() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.hset(sourceKey, String.valueOf(num), String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceSize = commands.hlen(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceSize);
            assertEquals(sourceSize.longValue(), 10_000L);

            ScanStream.hscan(commands, sourceKey)
                    .filter(Value::hasValue)
                    .map(Value::getValue)
                    .flatMap(num -> Flux.fromStream(IntStream.range(0, 4).boxed()).map(sfx -> num + "-" + sfx))
                    .concatMap(item -> commands.sadd(targetKey, item))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 40_000L);

        } finally {
            client.shutdown();
        }
    }

    /**
     * same test as before but using regular set as source
     *
     * THIS PASSES
     */
    @Test
    public void test8() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.sadd(sourceKey, String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceSize = commands.scard(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceSize);
            assertEquals(sourceSize.longValue(), 10_000L);

            commands.smembers(sourceKey)
                    .flatMap(num -> Flux.fromStream(IntStream.range(0, 4).boxed()).map(sfx -> num + "-" + sfx))
                    .concatMap(item -> commands.sadd(targetKey, item))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 40_000L);

        } finally {
            client.shutdown();
        }
    }

    /**
     * even simpler version, this reads from a hash and writes every other element to the target set
     *
     * THIS TEST FAILS
     */
    @Test
    public void test9() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.hset(sourceKey, String.valueOf(num), String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceSize = commands.hlen(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceSize);
            assertEquals(sourceSize.longValue(), 10_000L);

            ScanStream.hscan(commands, sourceKey)
                    .map(KeyValue::getValue)
                    .map(Integer::parseInt)
                    .filter(num -> num % 2 == 0)
                    .concatMap(item -> commands.sadd(targetKey, String.valueOf(item)))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 5_000L);

        } finally {
            client.shutdown();
        }
    }

    /**
     * even simpler version, this reads from a hash and writes every other element to the target set
     * using flatMap()
     *
     * THIS TEST FAILS
     */
    @Test
    public void test10() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.hset(sourceKey, String.valueOf(num), String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceSize = commands.hlen(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceSize);
            assertEquals(sourceSize.longValue(), 10_000L);

            ScanStream.hscan(commands, sourceKey)
                    .map(KeyValue::getValue)
                    .map(Integer::parseInt)
                    .filter(num -> num % 2 == 0)
                    .flatMap(item -> commands.sadd(targetKey, String.valueOf(item)))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 5_000L);

        } finally {
            client.shutdown();
        }
    }
    /**
     * this is a workaround for the case with hash as a source and flatMap()
     */
    @Test
    public void testAA() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            Flux.fromStream(IntStream.range(0, 10_000).boxed())
                    .flatMap(num -> commands.hset(sourceKey, String.valueOf(num), String.valueOf(num)))
                    .blockLast(Duration.ofSeconds(10));

            final Long sourceSize = commands.hlen(sourceKey).block(Duration.ofSeconds(10));
            assertNotNull(sourceSize);
            assertEquals(sourceSize.longValue(), 10_000L);

            ScanStream.hscan(commands, sourceKey)
                    .map(Value::getValue)
                    .flatMap(num -> Flux.fromStream(IntStream.range(0, 4).boxed())
                            .flatMap(sfx -> commands.sadd(targetKey, num + "-" + sfx)))
                    .blockLast(Duration.ofSeconds(10));

            final Long targetCard = commands.scard(targetKey).block(Duration.ofSeconds(10));
            assertNotNull(targetCard);
            assertEquals(targetCard.longValue(), 40_000L);

        } finally {
            client.shutdown();
        }
    }

}
