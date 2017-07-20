package nl.knaw.huygens.pergamon.janus;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

public class TestElasticBackend {
  private static final String ANN_INDEX = "janus_test_annotations";
  private static final String ANN_TYPE = "janus_test_annotation";
  private static final String DOC_INDEX = "janus_test_docs";
  private static final String DOC_TYPE = "janus_test_doc";

  private boolean available = true;
  private ElasticBackend backend;

  @Before
  public void connect() throws IOException {
    backend = new ElasticBackend("localhost", DOC_INDEX, DOC_TYPE, ANN_INDEX, ANN_TYPE);
    try {
      backend.initIndices();
    } catch (NoNodeAvailableException e) {
      available = false;
    }
    assumeTrue(available);
  }

  @After
  public void cleanup() throws Exception {
    if (available) {
      backend.removeIndices();
    }
    backend.close();
  }

  @Test
  public void nonExistent() throws IOException {
    DocAndAnnotations result = backend.getWithAnnotations("nothing here", false);
    assertEquals(null, result);
  }

  @Test
  public void txtAndAnnotation() throws Exception {
    Backend.PutResult result = backend.putTxt("some_id", "some text");
    assertEquals(201, result.status);
    assertEquals("some_id", result.id);

    result = backend.putTxt("some_id", "some other text");
    assertEquals(409, result.status);

    Annotation ann = new Annotation(0, 4, "some_id", "note", null, "test", null);
    result = backend.putAnnotation(ann);
    assertEquals(201, result.status);

    retry(3, 300, () -> {
      DocAndAnnotations dAndA = backend.getWithAnnotations("some_id", true);
      assertEquals("some text", dAndA.text);
      assertEquals(1, dAndA.annotations.size());

      String annId = dAndA.annotations.get(0).id;
      ann.id = annId;
      Annotation got = backend.getAnnotation(annId);
      assertEquals(ann, got);
      assertEquals("note", got.tag);
      assertEquals("note", dAndA.annotations.get(0).tag);
    });
  }

  @Test
  public void xml() throws Exception {
    String docId = "blabla!";
    String text = "<msg num=\"1\">hello, <xml num=\"2\" attr=\"extra\"/> world!</msg>";

    Backend.PutResult result = backend.putXml(docId, text);
    assertEquals(201, result.status);

    DocAndAnnotations[] doc = new DocAndAnnotations[1];
    retry(3, 300, () -> {
      doc[0] = backend.getWithAnnotations(result.id, true);
      assertEquals("hello,  world!", doc[0].text);
      assertEquals(2, doc[0].annotations.size());
    });

    List<Annotation> ann = doc[0].annotations;
    assertEquals(new Annotation(0, 14, docId, "msg", null, "tag", ann.get(0).id, ImmutableMap.of("num", "1")),
      ann.get(0));
    assertEquals(new Annotation(7, 7, docId, "xml", null, "tag", ann.get(1).id,
        ImmutableMap.of("num", "2", "attr", "extra")),
      ann.get(1));
  }

  @Test
  public void xmlNullId() throws Exception {
    Backend.PutResult result = backend.putXml(null, "<hello>world</hello>");
    assertEquals(201, result.status);
    assertNotNull(result.id);
  }

  private interface Assertion {
    void run() throws Exception;
  }

  // Retries an assertion at most repeats times, sleeping millis in between.
  private static void retry(int repeats, long millis, Assertion assertion) throws Exception {
    for (int i = 0; i < repeats - 1; i++) {
      try {
        assertion.run();
        break;
      } catch (AssertionError e) {
        Thread.sleep(millis);
      }
    }
    assertion.run();
  }
}
