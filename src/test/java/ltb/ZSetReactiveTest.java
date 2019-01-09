package ltb;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

/**
 * Copyright (c) 2019 Happy Gears
 * author: vadim2
 * Date: 2019-01-09
 */
public class ZSetReactiveTest {

    private final String set1 = "test.source";
    private final String set2 = "test.target";

    private void makeSets(RedisReactiveCommands<String, String> commands) {
        // create sorted set 1, members are "a1", "a2", "a3", etc, with scores 1,2,3 etc

        Flux.fromStream(IntStream.range(0, 5_000).boxed())
                .flatMap(num -> commands.zadd(set1, ScoredValue.from(num, Optional.of("a" + num))))
                .blockLast(Duration.ofSeconds(10));

        // create sorted set 2, members are "a1", "a2", "a3", etc, with scores 100,200,300 etc

        Flux.fromStream(IntStream.range(0, 5_000).boxed())
                .flatMap(num -> commands.zadd(set2, ScoredValue.from(num * 100, Optional.of("a" + num))))
                .blockLast(Duration.ofSeconds(10));
    }

    /**
     * method 1: read members, then run zip'ped Flux to issue ZSCORE commands against two sets
     */
    @Test
    public void test1() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            makeSets(commands);

            final Long size1 = commands.zcard(set1).block(Duration.ofSeconds(1));
            final Long size2 = commands.zcard(set2).block(Duration.ofSeconds(1));

            assertNotNull(size1);
            assertEquals(size1.longValue(), 5000);

            assertNotNull(size2);
            assertEquals(size2.longValue(), 5000);

            final List<String> members = commands.zrange(set1, 0, -1)
                    .collectList()
                    .block(Duration.ofSeconds(1));

            final Long count = Flux.fromIterable(members)
                    .flatMap(member -> Flux.zip(Flux.just(member), commands.zscore(set1, member), commands.zscore(set2, member)))
                    .count()
                    .block(Duration.ofSeconds(2));
            assertNotNull(count);
            assertEquals(count.longValue(), 5000L);
        }

    }

    /**
     * method 2: zrangeWithScores() from one set, then call zipWith to get scores from the other set
     *
     * this fails with Lettuce 5.1.3.RELEASE
     */
    @Test
    public void test2() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            makeSets(commands);

            final Long size1 = commands.zcard(set1).block(Duration.ofSeconds(1));
            final Long size2 = commands.zcard(set2).block(Duration.ofSeconds(1));

            assertNotNull(size1);
            assertEquals(size1.longValue(), 5000);

            assertNotNull(size2);
            assertEquals(size2.longValue(), 5000);

            final Long count = commands.zrangeWithScores(set1, 0, -1)
                    .flatMap(sv -> {
                        final String member = sv.getValue();
                        return Mono.zip(commands.zscore(set2, member), Mono.just(sv));
                    })
                    .count()
                    .block(Duration.ofSeconds(2));
            assertNotNull(count);
            assertEquals(count.longValue(), 5000L);
        }

    }
}
