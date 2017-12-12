package nl.knaw.huygens.pergamon.janus;

import com.google.common.collect.ImmutableMap;
import io.dropwizard.jackson.Jackson;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;

public class TestElasticBackendIntegration {
  private static final String ANN_INDEX = "janus_test_annotations";
  private static final String ANN_TYPE = "janus_test_annotation";
  private static final String DOC_INDEX = "janus_test_docs";
  private static final String DOC_TYPE = "janus_test_doc";

  private static boolean available = true;
  private static ElasticBackend backend;

  private static Path tempDir;

  @BeforeClass
  public static void connect() throws IOException {
    try {
      tempDir = Files.createTempDirectory("TestElasticBackendIntegration");
      Mapping mapping = new Mapping(asList(
        new Mapping.Field("body", "text", "/"),
        new Mapping.Field("author", "keyword", "//author"),
        new Mapping.Field("receiver", "keyword", "//receiver")
      ), false);
      backend = new ElasticBackend(Collections.emptyList(), DOC_INDEX, DOC_TYPE, ANN_INDEX, ANN_TYPE, mapping, tempDir);
      backend.initIndices();
    } catch (ConnectException e) {
      available = false;
    }
    assumeTrue(available);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    if (available) {
      backend.removeIndices();
    }
    backend.close();
    Files.walk(tempDir)
         .sorted(Comparator.reverseOrder()) // parents first
         .map(Path::toFile)
         .forEach(File::delete);
  }

  @Test
  public void nonExistent() throws IOException {
    DocAndAnnotations result = backend.getWithAnnotations("nothing-here", false);
    assertEquals(null, result);
  }

  @Test
  public void noIndexYet() throws IOException {
    Path tmp = null;
    try {
      tmp = Files.createTempDirectory("janus-TestElasticBackendIntegration");
      ElasticBackend newBackend = new ElasticBackend(Collections.emptyList(),
        "surely-nonexistent-doc-index", "surely-nonexistent-doc-type", ANN_INDEX, ANN_TYPE, null, tmp);

      // These shouldn't raise IndexNotFoundException.
      assertEquals(null, newBackend.getWithAnnotations("nothing-here", false));
      assertEquals(ElasticBackend.ListPage.empty(), newBackend.listDocs("query", 0, 1));
    } finally {
      Files.walk(tmp)
           .sorted(Comparator.reverseOrder()) // parents first
           .map(Path::toFile)
           .forEach(File::delete);
    }
  }

  @Test
  public void txtAndAnnotation() throws Exception {
    ElasticBackend.PutResult result = backend.putTxt("some_id", "some text");
    assertEquals(201, result.status);
    assertEquals("some_id", result.id);

    result = backend.putTxt("some_id", "some other text");
    assertEquals(409, result.status);

    Annotation ann = new Annotation(0, 4, "some_id", "note", null, "test", null);
    result = backend.putAnnotation(ann);
    assertEquals(201, result.status);

    retry(() -> {
      DocAndAnnotations dAndA = backend.getWithAnnotations("some_id", true);
      assertEquals("some_id", dAndA.id);
      assertEquals("some text", dAndA.text);
      assertEquals(1, dAndA.annotations.size());

      String annId = dAndA.annotations.get(0).id;
      ann.id = annId;
      Annotation got = backend.getAnnotation(annId);
      assertEquals(ann, got);
      assertEquals("note", got.type);
      assertEquals("note", dAndA.annotations.get(0).type);
    });
  }

  @Test
  public void xml() throws Exception {
    String docId = "blabla";
    String text = "<msg num=\"1\"><author>tester</author> says hello, <xml num=\"2\" attr=\"extra\"/> world!</msg>";

    ElasticBackend.PutResult result = backend.putXml(docId, text);
    assertEquals(201, result.status);

    DocAndAnnotations doc = retry(() -> {
      DocAndAnnotations d = backend.getWithAnnotations(result.id, true);
      assertEquals("tester says hello,  world!", d.text);
      assertEquals(d.text, d.body.get("body"));
      assertEquals(3, d.annotations.size());
      return d;
    });

    assertEquals(docId, doc.id);

    List<Annotation> ann = doc.annotations;
    assertEquals(new Annotation(0, 26, docId, "msg", null, "xml", ann.get(0).id, ImmutableMap.of("num", "1")),
      ann.get(0));
    assertEquals(new Annotation(0, 6, docId, "author", null, "xml", ann.get(1).id, emptyMap()),
      ann.get(1));
    assertEquals(new Annotation(19, 19, docId, "xml", null, "xml", ann.get(2).id,
        ImmutableMap.of("num", "2", "attr", "extra")),
      ann.get(2));

    RestStatus r = backend.delete(docId);
    assertEquals(200, r.getStatus());

    retry(() -> {
      SearchResponse resp =
        backend.hiClient.search(new SearchRequest(ANN_INDEX)
          .types(ANN_TYPE).source(SearchSourceBuilder.searchSource()
                                                     .query(new TermQueryBuilder("root", docId))));
      assertEquals(0, resp.getHits().getHits().length);
    });

    int status = backend.updateXml(docId, "<msg num=\"2\"></msg>").status;
    assertEquals(201, status);
  }

  @Test
  public void annotationOnAnnotation() throws Exception {
    String docId = backend.putTxt(null, "root doc").id;
    assertNotNull(docId);

    Annotation ann1 = new Annotation(0, 21, docId, "level1", null, "test", null);
    ElasticBackend.PutResult result = backend.putAnnotation(ann1);
    assertEquals(result.status, 201);
    assertNotNull(result.id);
    ann1.id = result.id;

    Annotation ann2 = new Annotation(0, 0, ann1.id, "level2", null, "test", null);
    result = backend.putAnnotation(ann2);
    assertNotNull(result.id);
    ann2.id = result.id;

    retry(() -> assertEquals(ann1, backend.getAnnotation(ann1.id)));
    retry(() -> assertEquals(ann2, backend.getAnnotation(ann2.id)));
  }

  @Test
  public void addBody() throws Exception {
    String docId = backend.putTxt(null, "some doc").id;
    assertNotNull(docId);

    Annotation ann = new Annotation(0, 4, docId, "note", null, "test", null);
    ElasticBackend.PutResult result = backend.putAnnotation(ann);
    assertEquals(result.status, 201);
    String annid = result.id;
    assertNotNull(annid);

    result = backend.putTxt(null, "body of note");
    assertEquals(result.status, 201);
    assertNotNull(result.id);
    String bodyid = result.id;
    Response response = backend.addBody(annid, bodyid);
    assertEquals(200, response.getStatus());

    ann.body = bodyid;
    ann.id = annid;
    retry(() -> assertEquals(ann, backend.getAnnotation(annid)));
  }

  @Test
  public void attributes() throws Exception {
    String docId = backend.putTxt(null, "don't care").id;
    assertNotNull(docId);

    Annotation ann = new Annotation(6431, 121261, docId, "withattr", null, "test", null,
      ImmutableMap.of("key", "value"
        , "nested.key", "nested.value" // nested according to Elasticsearch, Janus doesn't care
        , "deeply.nested.key", "deeply.nested.value"
        , " foo .bar ", " baz  . quux "
        // TODO broken because of Elasticsearch mapping:
        // , " ", " "
        // , "..", "..."
        // , "", ""
      ));
    ann.id = backend.putAnnotation(ann).id;

    retry(() -> {
      Annotation got = backend.getAnnotation(ann.id);
      assertEquals(ann, got);
    });
  }

  private static ElasticBackend.PutResult put(String s) throws Exception {
    ElasticBackend.PutResult r = backend.putTxt(null, s);
    if (r.status != 201) {
      throw new Exception(String.format("failure: %d, %s", r.status, r.message));
    }
    return r;
  }

  private static String q(String s) {
    return s.replace('`', '"');
  }

  @Test
  public void search() throws Exception {
    // Both documents and queries must allow full UTF-8.
    put("René Descartes");
    put("π = 3.14159");

    org.elasticsearch.client.Response r = backend.search(q("{`query`: {`query_string`: {`query`: `René OR π`}}}"));
    Map m = Jackson.newObjectMapper().readValue(r.getEntity().getContent(), Map.class);
    assertFalse(m.containsKey("error"));

    // Elasticsearch status codes should be passed on.
    r = backend.search(q("{`foo`: `bar`}"));
    assertEquals(400, r.getStatusLine().getStatusCode());

    r = backend.search("{foo}");
    assertEquals(500, r.getStatusLine().getStatusCode());
  }

  @Test
  public void xmlNullId() throws Exception {
    String xml = "<hello>world</hello>";
    String id = putXml(xml);
    String stored = backend.getOriginal(id);
    assertEquals(xml, stored);
  }

  // @Test
  // public void cooccurrence() throws Exception {
  //   putXml("<foo><author>1</author><receiver>2</receiver></foo>");
  //   putXml("<foo><author>2</author><receiver>1</receiver></foo>");
  //   putXml("<foo><author>1</author><receiver>1</receiver></foo>");
  //
  //   retry(() -> {
  //     List<Map<String, Object>> r =
  //       backend.cooccurrence(
  //         new HashMap<String, Object>() {{
  //           put("match_all", Collections.emptyMap());
  //         }}, "author", "receiver")
  //              .stream()
  //              .sorted(comparing(m -> (long) m.get("weight")))
  //              .collect(toList());
  //
  //     assertEquals(2, r.size());
  //     assertEquals(1L, r.get(0).get("weight"));
  //     assertEquals("1", r.get(0).get("source"));
  //     assertEquals("1", r.get(0).get("target"));
  //     assertEquals(2L, r.get(1).get("weight"));
  //     assertEquals("1", r.get(1).get("source"));
  //     assertEquals("2", r.get(1).get("target"));
  //   });
  // }

  private String putXml(String xml) throws IOException {
    ElasticBackend.PutResult result = backend.putXml(null, xml);
    assertEquals(result.message, 201, result.status);
    assertNotNull(result.id);
    return result.id;
  }

  // Block of code with assertions. May return a value for convenience.
  private interface Assertion<T> {
    T run() throws Exception;
  }

  private interface VoidAssertion<T> {
    void run() throws Exception;
  }

  private static int REPEATS = 10;
  private static long WAIT = 300; // milliseconds

  // Retries an assertion at most repeats times, sleeping millis in between.
  private static <T> T retry(Assertion<T> assertion) throws Exception {
    for (int i = 1; i < REPEATS; i++) {
      try {
        return assertion.run();
      } catch (AssertionError e) {
        Thread.sleep(WAIT);
      }
    }
    return assertion.run();
  }

  private static void retry(VoidAssertion assertion) throws Exception {
    retry(() -> {
      assertion.run();
      return null;
    });
  }
}
