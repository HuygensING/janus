package nl.knaw.huygens.pergamon.janus;

import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
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
  public void putTxt() throws IOException {
    Backend.PutResult result = backend.putTxt("some_id", "some text");
    assertEquals(201, result.status);
    assertEquals("some_id", result.id);

    result = backend.putTxt("some_id", "some other text");
    assertEquals(409, result.status);
  }
}
