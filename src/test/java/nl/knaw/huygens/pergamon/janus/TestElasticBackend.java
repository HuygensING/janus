package nl.knaw.huygens.pergamon.janus;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static java.lang.Thread.sleep;
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

    result = backend.putAnnotation(new Annotation(0, 4, "some_id", "note", null, "test", null));
    assertEquals(201, result.status);

    sleep(500);

    DocAndAnnotations dAndA = backend.getWithAnnotations("some_id", true);
    assertEquals("some text", dAndA.text);
    assertEquals(1, dAndA.annotations.size());

    String annId = dAndA.annotations.get(0).id;
    Annotation ann = backend.getAnnotation(annId);
    assertNotNull(ann);
  }

  @Test
  public void xml() throws Exception {
    Backend.PutResult result = backend.putXml("blabla!", "<msg>hello, <xml/> world!</msg>");
    assertEquals(201, result.status);

    sleep(500); // ES may handle get before document is properly indexed

    DocAndAnnotations doc = backend.getWithAnnotations(result.id, true);
    assertEquals("hello,  world!", doc.text);
    assertEquals(2, doc.annotations.size());
    assertEquals("msg", doc.annotations.get(0).tag);
    assertEquals("xml", doc.annotations.get(1).tag);
  }

  @Test
  public void xmlNullId() throws Exception {
    Backend.PutResult result = backend.putXml(null, "<hello>world</hello>");
    assertEquals(201, result.status);
    assertNotNull(result.id);
  }
}
