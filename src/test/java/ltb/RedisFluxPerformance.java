package ltb;

import com.google.common.base.Stopwatch;
import io.lettuce.core.ClientOptions;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.reactive.RedisReactiveCommands;
import io.lettuce.core.resource.ClientResources;
import io.lettuce.core.resource.DefaultClientResources;
import org.reactivestreams.Publisher;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.fail;

public class RedisFluxPerformance {

    private ClientResources clientResources;
    private RedisClient client;
    private Random random = new Random(31416);

    @BeforeClass
    public void setUp() {
        clientResources = DefaultClientResources.builder()
                .ioThreadPoolSize(8)
                .computationThreadPoolSize(8)
                .build();

        client = RedisClient.create(clientResources, "redis://localhost:6380");
        final ClientOptions co = ClientOptions.builder()
                .autoReconnect(true)                        // actually this is the default
//                .publishOnScheduler(true)
                .build();
        client.setOptions(co);
        client.setDefaultTimeout(Duration.of(10, ChronoUnit.SECONDS));
    }

    @AfterClass
    public void tearDown() {
        clientResources.shutdown();
    }


    /**
     * run a reactive drain test with the given equipment and return a halted stopwatch
     * containing the elapsed time of the test, exclusive of data preparation and enqueueing.
     *
     * I admit, the mapper part is excessively tricky, but it was interesting to figure it
     * out. The goal is to allow the caller to select between a flatMap and concatMap
     * step. Each of those functions takes an A -> Flux&lt;A&gt; argument, is applied to
     * a flux, and produces a flux. Here we simplify by having the input and output types
     * the same, so the type of mapper is:
     *
     * (A -> Flux&lt;A&gt;) -> Flux&lt;A&gt; -> Flux&lt;A&gt;.
     *
     * Such a function can be supplied to a compose step in a reactive chain.
     *
     * @param nItems How many MVarStubs to ship downstream.
     * @param fluxSupplier how to transform Queue into Flux.
     * @param nConnections how many Redis connections in round robin pool (may be 1).
     * @param mapper effectively, either a flatMap or concatMap step, in the form of a function.
     *
     */
    private Stopwatch multiTest(
            final int nItems,
            final Function<BlockingQueue<MVarStub>, Flux<MVarStub>> fluxSupplier,
            final int nConnections,
            Function<Function<MVarStub, Publisher<MVarStub>>, Function<Flux<MVarStub>, Flux<MVarStub>>> mapper

    ) {
        final Semaphore semaphore = new Semaphore(0);
        final LinkedBlockingQueue<MVarStub> q = Stream
                .generate(() -> MVarStub.newRandom(512, 16, 4096))
                .limit(nItems)
                .collect(Collectors.toCollection(LinkedBlockingQueue::new));
        final RRPool rrPool = new RRPool(client, nConnections);
        Function<MVarStub, Publisher<MVarStub>> sender = stub -> send(stub, rrPool);
        // I admit, this is probably overly tricky. The purpp
        // Function<Function<MVarStub, Publisher<MVarStub>>, Function<Flux<MVarStub>, Flux<MVarStub>>> mapper = suite -> flux -> flux.concatMap(suite);

        Stopwatch sw = Stopwatch.createStarted();
        fluxSupplier.apply(q)
                //.publishOn(Schedulers.parallel())  // critical, it locks up if I do not call .publishOn()
                //.filter(this::markers)
                //.filter(stub1 -> filterIfShouldSave(stub1, false))
                //.concatMap(sender)
//                .onErrorContinue((throwable, o) ->
//                        log.error("Error in write queue reactive chain: object=" + o + ", error=", throwable))
                .compose(mapper.apply(sender))
                //.concatMap(sender)
                .take(nItems)
                .subscribe(
                        stub -> {},
                        t -> fail("subscription", t),
                        semaphore::release
                );

        semaphore.acquireUninterruptibly();
        sw.stop();
        System.out.format("%s done in %s : %f/sec", nItems, sw, 1e9f * nItems / sw.elapsed(TimeUnit.NANOSECONDS));
        rrPool.shutdown();
        return sw;
    }

    @Test
    public void PlainQueue1ConnConcat() {
        multiTest(64 * 1024, Flux::fromIterable, 1, sender -> (flux -> flux.concatMap(sender)));
    }

    @Test
    public void ReactiveQueue1ConnConcat() {
        multiTest(64 * 1024, ReactiveQueueAdapter::of, 1, sender -> (flux -> flux.concatMap(sender)));
    }

    @Test
    public void PlainQueue2ConnConcat() {
        multiTest(64 * 1024, Flux::fromIterable, 2, sender -> (flux -> flux.concatMap(sender)));
    }

    @Test
    public void ReactiveQueue2ConnConcat() {
        multiTest(64 * 1024, ReactiveQueueAdapter::of, 2, sender -> (flux -> flux.concatMap(sender)));
    }

    @Test
    public void PlainQueue1ConnFlat() {
        multiTest(64 * 1024, Flux::fromIterable, 1, sender -> (flux -> flux.flatMap(sender)));
    }

    @Test
    public void ReactiveQueue1ConnFlat() {
        multiTest(64 * 1024, ReactiveQueueAdapter::of, 1, sender -> (flux -> flux.flatMap(sender)));
    }

    @Test
    public void PlainQueue2ConnFlat() {
        multiTest(64 * 1024, Flux::fromIterable, 2, sender -> (flux -> flux.flatMap(sender)));
    }

    @Test
    public void ReactiveQueue2ConnFlat() {
        multiTest(64 * 1024, ReactiveQueueAdapter::of, 2, sender -> (flux -> flux.flatMap(sender)));
    }

    @Test
    public void smokeTest() {
        final StatefulRedisConnection<String, String> conn = client.connect();
        final Semaphore semaphore = new Semaphore(0);

        conn.reactive().keys("*").count().subscribe(
                k -> System.out.println("found " + k + " keys"),
                e -> fail("test1", e),
                semaphore::release
        );

        semaphore.acquireUninterruptibly();
        conn.close();
    }

    private static class RRPool implements Supplier<RedisReactiveCommands<String, String>> {
        private final int n;
        private final List<StatefulRedisConnection<String, String>> conns;
        private int ix = 0;

        RRPool(RedisClient client, int n) {
            this.n = n;
            conns = new ArrayList<>(n);
            for (int i = 0; i < n; i++) conns.add(client.connect());
        }

        @Override
        public RedisReactiveCommands<String, String> get() {
            StatefulRedisConnection<String, String> c = conns.get(ix);
            ix = (ix+1) % n;
            return c.reactive();
        }

        void shutdown() {
            for (StatefulRedisConnection<String, String> conn : conns) conn.close();
        }
    }

    private final String area = "Area51";

    private Mono<MVarStub> send(MVarStub stub, Supplier<RedisReactiveCommands<String, String>> reactive) {

        final String key = String.format("%s:%s", area, stub.varName);
        final String triplet = stub.triplet();


        return reactive.get().hset(key, triplet, stub.serialize())
                .checkpoint("mvar-stubs-storage-2-send-1")
                .map(tuple3 -> stub)
                //.doOnNext(this::postSend)
                .doOnError(e -> {
                    fail("send", e);
                })
                .checkpoint("mvar-stubs-storage-2-send-2");
    }


}
