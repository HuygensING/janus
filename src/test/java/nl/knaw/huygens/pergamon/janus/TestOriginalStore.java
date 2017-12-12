package nl.knaw.huygens.pergamon.janus;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;

public class TestOriginalStore {
  @Test
  public void testHash() throws IOException {
    // SHA-256 of empty string = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.
    assertEquals(0xe3b0, OriginalStore.hash(""));

    Path tmp = null;
    try {
      tmp = Files.createTempDirectory("janus-TestOriginalStore");
      OriginalStore store = new OriginalStore(tmp, 200);

      assertEquals(tmp.resolve("c3/ab/foobar"), store.getPath("foobar"));
    } finally {
      Files.delete(tmp);
    }
  }
}
