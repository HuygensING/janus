package nl.knaw.huygens.pergamon.janus;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class TestOriginalStore {
  private static OriginalStore store = null;
  private static Path tmpdir = null;

  @BeforeClass
  public static void setup() throws IOException {
    tmpdir = Files.createTempDirectory("janus-TestOriginalStore");
    store = new OriginalStore(tmpdir, 100);
  }

  @AfterClass
  public static void teardown() throws IOException {
    Files.walk(tmpdir)
         .sorted(Comparator.reverseOrder()) // parents first
         .map(Path::toFile)
         .forEach(File::delete);
    store = null;
    tmpdir = null;
  }

  @Test
  public void testHash() throws IOException {
    // SHA-256 of empty string = e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855.
    assertEquals(0xe3b0, OriginalStore.hash(""));
    int h = OriginalStore.hash("foobar");
    assertEquals(tmpdir.resolve("c3/ab/foobar"), store.getPath("foobar", h));
  }

  @Test
  public void writer() throws IOException, TimeoutException {
    try (OriginalStore.WriteOp put = store.put("hello", "hello, world!")) {
      put.commit();
    }
    assertEquals("hello, world!", new String(store.get("hello")));

    try (OriginalStore.WriteOp replace = store.replace("hello", "goodbye!")) {
      assertFalse(replace.noop());
      replace.commit();
    }
    assertEquals("goodbye!", new String(store.get("hello")));
  }
}
