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

      return null;
    });
  }

  @Test
  public void xml() throws Exception {
    String docId = "blabla!";
    String text = "<msg num=\"1\">hello, <xml num=\"2\" attr=\"extra\"/> world!</msg>";

    Backend.PutResult result = backend.putXml(docId, text);
    assertEquals(201, result.status);

    DocAndAnnotations doc = retry(3, 300, () -> {
      DocAndAnnotations d = backend.getWithAnnotations(result.id, true);
      assertEquals("hello,  world!", d.text);
      assertEquals(2, d.annotations.size());
      return d;
    });

    List<Annotation> ann = doc.annotations;
    assertEquals(new Annotation(0, 14, docId, "msg", null, "tag", ann.get(0).id, ImmutableMap.of("num", "1")),
      ann.get(0));
    assertEquals(new Annotation(7, 7, docId, "xml", null, "tag", ann.get(1).id,
        ImmutableMap.of("num", "2", "attr", "extra")),
      ann.get(1));
  }

  @Test
  public void annotationOnAnnotation() throws Exception {
    String docId = backend.putTxt(null, "root doc").id;
    assertNotNull(docId);

    Annotation ann1 = new Annotation(0, 21, docId, "level1", null, "test", null);
    String annId1 = retry(7, 200, () -> {
      Backend.PutResult result = backend.putAnnotation(ann1);
      assertEquals(result.status, 201);
      assertNotNull(result.id);
      return result.id;
    });
    ann1.id = annId1;

    Annotation ann2 = new Annotation(0, 0, annId1, "level2", null, "test", null);
    String annId2 = retry(7, 200, () -> {
      String id = backend.putAnnotation(ann2).id;
      assertNotNull(id);
      return id;
    });
    ann2.id = annId2;

    assertEquals(ann1, backend.getAnnotation(annId1));
    assertEquals(ann2, backend.getAnnotation(annId2));
  }

  @Test
  public void attributes() throws Exception {
    String docId = backend.putTxt(null, "don't care").id;
    assertNotNull(docId);

    Annotation ann = new Annotation(6431, 121261, docId, "withattr", null, "test", null,
      ImmutableMap.of("key", "value", "nested.key", "nested.value"
        // TODO broken because of Elasticsearch mapping:
        // , "..", "..."
        // , "", ""
      ));
    ann.id = backend.putAnnotation(ann).id;

    Annotation got = retry(7, 200, () -> backend.getAnnotation(ann.id));

    assertEquals(ann, got);
  }

  @Test
  public void xmlNullId() throws Exception {
    Backend.PutResult result = backend.putXml(null, "<hello>world</hello>");
    assertEquals(201, result.status);
    assertNotNull(result.id);
  }

  // Block of code with assertions. May return a value for convenience.
  private interface Assertion<T> {
    T run() throws Exception;
  }

  // Retries an assertion at most repeats times, sleeping millis in between.
  private static <T> T retry(int repeats, long millis, Assertion<T> assertion) throws Exception {
    for (int i = 1; i < repeats; i++) {
      try {
        return assertion.run();
      } catch (AssertionError e) {
        Thread.sleep(millis);
      }
    }
    return assertion.run();
  }
}
