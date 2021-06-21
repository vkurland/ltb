/*
 * Copyright (c) 2019 Happy Gears, Inc.
 * author: colin
 * Date: 3/20/2019
 *
 */
package ltb;

import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class ReactiveLoggerTest {
    @Test
    public void testSimple() {
        List<String> log = new ArrayList<>();
        ReactiveLogger<Integer> rlog = new ReactiveLogger<>("foo", l -> {
            log.add(l);
            System.out.println(l);
        });
        List<Integer> outs = new ArrayList<>();
        Flux.range(1, 20)
                .compose(rlog)
                .subscribe(outs::add);
        assertEquals(log.size(), 1);
        assertTrue(log.get(0).contains("count 20"));
        assertEquals(outs, Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20));
    }

    @Test
    public void testBeforeAndAfterFilter() {
        List<String> log = new ArrayList<>();
        ReactiveLogger<Integer> rlog1 = new ReactiveLogger<>("foo", l -> {
            log.add(l);
            System.out.println(l);
        });
        ReactiveLogger<Integer> rlog2 = new ReactiveLogger<>("bar", l -> {
            log.add(l);
            System.out.println(l);
        });
        List<Integer> outs = new ArrayList<>();
        Flux.range(1, 20)
                .compose(rlog1)
                .filter(j -> j % 2 == 0)
                .compose(rlog2)
                .subscribe(outs::add);
        assertEquals(log.size(), 2);
        assertTrue(log.get(0).contains("count 20"));
        assertTrue(log.get(1).contains("count 10"));
        assertEquals(outs, Arrays.asList(2, 4, 6, 8, 10, 12, 14, 16, 18, 20));
    }

    @Test
    public void useSameInstanceTwice() {
        List<String> log = new ArrayList<>();
        ReactiveLogger<Integer> rlog = new ReactiveLogger<>("twice", l -> {
            log.add(l);
            System.out.println(l);
        });
        List<Integer> outs = new ArrayList<>();
        Flux.range(1, 20)
                .compose(rlog)
                .filter(j -> j % 2 == 0)
                .compose(rlog)
                .subscribe(outs::add);
        assertEquals(outs, Arrays.asList(2, 4, 6, 8, 10, 12, 14, 16, 18, 20));
        assertEquals(log.size(), 2);
        assertTrue(log.get(0).contains("count 20"));
        assertTrue(log.get(1).contains("count 10"));
    }

    @Test
    public void worksWithEmpty() {
        List<String> log = new ArrayList<>();
        ReactiveLogger<String> rlog = new ReactiveLogger<>("huh?", l -> {
            log.add(l);
            System.out.println(l);
        });
        Flux.<String>empty().compose(rlog).subscribe();
        assertEquals(log.size(), 1);
    }

    @Test
    public void worksWithError() {
        List<String> log = new ArrayList<>();
        final ReactiveLogger<Object> rlog = new ReactiveLogger<>("oops!", l -> {
            log.add(l);
            System.out.println(l);
        });
        Flux.error(new IllegalStateException("your own private Idaho"))
                .compose(rlog)
                .onErrorResume(t -> Flux.just(4, 3, 2))
                .compose(rlog)
                .subscribe();
        assertEquals(log.size(), 2);
        assertTrue(log.get(0).contains("count 0 error"));
        assertTrue(log.get(1).contains("count 3 complete"));
    }

    @Test
    public void worksWithStuffThenError() {
        List<String> log = new ArrayList<>();
        List<Integer> outs = new ArrayList<>();
        Flux.range(1, 5)
                .concatWith(Flux.error(new IllegalStateException("your own private Idaho")))
                .compose(new ReactiveLogger<>("and...", l -> {
                    log.add(l);
                    System.out.println(l);
                }))
                .onErrorReturn(99)
                .subscribe(outs::add);
        assertEquals(log.size(), 1);
        assertTrue(log.get(0).contains("count 5 error"));
        assertEquals(outs, Arrays.asList(1, 2, 3, 4, 5, 99));
    }
}

