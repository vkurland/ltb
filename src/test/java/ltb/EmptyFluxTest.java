package ltb;

import org.testng.annotations.Test;
import reactor.core.publisher.Flux;

import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class EmptyFluxTest {

    @Test
    public void test1() {
        List<Integer> list = List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final Long count = Flux.fromIterable(list)
                .count()
                .block();
        assertEquals(count.intValue(), 10);
    }

    @Test
    public void test2() {
        List<Integer> list = List.of();
        final Long count = Flux.fromIterable(list)
                .count()
                .block();
        assertEquals(count.intValue(), 0);
    }

    @Test
    public void test3() {
        List<Integer> list = List.of();
        final Integer count = Flux.fromIterable(list)
                .reduce(Integer::sum)
                .block();
        assertNull(count);
    }
}
