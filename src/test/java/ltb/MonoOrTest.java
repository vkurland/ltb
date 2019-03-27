package ltb;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import org.testng.annotations.Test;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

/**
 * Copyright (c) 2018 Happy Gears
 * author: vadim2
 * Date: 10/21/18
 */
public class MonoOrTest {

    private final String sourceKey = "test.source";
    private final String targetKey = "test.target";

    private final Map<String, String> cache = new ConcurrentHashMap();

    @Test
    public void test2() {

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisReactiveCommands<String, String> commands = connection.reactive();

            commands.flushall().block();

            commands.set(targetKey, "TEST")
                    .block();

            assertNull(cache.get(targetKey));

            String result = Mono.justOrEmpty(cache.get(targetKey))
                    .or(commands.get(targetKey))
                    .block();

            assertEquals(result, "TEST");

        } finally {
            client.shutdown();
        }
    }

}
