package ltb;

import redis.embedded.RedisExecProvider;
import redis.embedded.RedisServer;
import redis.embedded.util.Architecture;
import redis.embedded.util.OS;

import java.io.File;
import java.io.IOException;

/**
 * Copyright (c) 2018 Happy Gears
 * author: vadim2
 * Date: 7/30/18
 */
public class TestRedisServer extends RedisServer {

    private static RedisServer redisServer = null;

    public static void startServer() {
        RedisExecProvider customProvider = RedisExecProvider.defaultProvider()
                .override(OS.UNIX, "/usr/bin/redis-server")
                .override(OS.MAC_OS_X, Architecture.x86_64, "/usr/local/bin/redis-server");

        try {
            redisServer = new TestRedisServer(customProvider, 6379);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }

        redisServer.start();

        System.setProperty("REDIS", "redis://localhost:6379");
    }

    public static void stopServer() {
        redisServer.stop();
    }

    public TestRedisServer() throws IOException {
    }

    public TestRedisServer(Integer port) throws IOException {
        super(port);
    }

    public TestRedisServer(File executable, Integer port) {
        super(executable, port);
    }

    public TestRedisServer(RedisExecProvider redisExecProvider, Integer port) throws IOException {
        super(redisExecProvider, port);
    }

    /**
     * change "ready to server" pattern to support Redis 4.x
     */
    @Override
    protected String redisReadyPattern() {
        return ".*[rR]eady to accept connections.*";
    }
}
