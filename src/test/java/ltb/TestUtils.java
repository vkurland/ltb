package ltb;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisServerReactiveCommands;

import java.io.IOException;

/**
 * Copyright (c) 2013 HappyGears
 * author: vadim
 * Date: 10/20/13
 */
public class TestUtils {

    static void startRedisServer() {
        System.out.println("Starting Redis server");
        Runtime.getRuntime().addShutdownHook(new Thread(TestUtils::stopRedisServer));
        // I assume that the machine on which this test runs is not supposed to run
        // permanent Redis server and the only redis server that can run is the one
        // we spawn here for the test. Therefore, I find and kill any redis-server
        // processes that might be running assuming they are left-overs from
        // the previous unsuccessful test run
        try {
            Runtime.getRuntime().exec("pkill redis-server");
        } catch (IOException e) {
            System.err.println(e);
        }
        TestRedisServer.startServer();
    }

    static void stopRedisServer() {
        System.out.println("Stopping Redis server");
        TestRedisServer.stopServer();
    }

    public static void flushRedis() {
        System.out.println("Redis flushall");

        final RedisClient client = RedisClient.create(RedisURI.create("127.0.0.1", 6379));

        try (StatefulRedisConnection<String, String> connection = client.connect()) {
            final RedisServerReactiveCommands<String, String> commands = connection.reactive();
            commands.flushall().block();
        }

        System.out.println("Redis data cleared");
    }

}
