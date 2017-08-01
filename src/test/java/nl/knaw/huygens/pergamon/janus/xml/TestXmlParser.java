package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.ParsingException;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;

public class TestXmlParser {
  private static final int NTHREADS = 50;

  @Test
  public void multithreaded() throws InterruptedException {
    String xml = range(0, 1000)
      .mapToObj(x -> "<all-work-and-no-play makes=\"jack\">a dull boy</all-work-and-no-play>")
      .collect(Collectors.joining("\n", "<root>", "</root>"));

    CountDownLatch latch = new CountDownLatch(NTHREADS);
    AtomicLong maxActive = new AtomicLong();

    for (int i = 0; i < NTHREADS; i++) {
      new Thread(() -> {
        long active = latch.getCount();
        maxActive.accumulateAndGet(active, Long::max);

        try {
          // Parse multiple times to test reuse of Builder.
          for (int j = 0; j < 10; j++) {
            XmlParser.fromString(xml);
          }
        } catch (IOException | ParsingException e) {
          throw new RuntimeException(e);
        } finally {
          latch.countDown();
        }
      }).run();
    }

    latch.await();

    //System.out.printf("max active threads: %d\n", maxActive.get());
  }
}
