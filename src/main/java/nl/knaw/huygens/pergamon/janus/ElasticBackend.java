package nl.knaw.huygens.pergamon.janus;

import com.codahale.metrics.health.HealthCheck;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.dropwizard.jackson.Jackson;
import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
import nl.knaw.huygens.pergamon.janus.xml.XmlParser;
import nu.xom.Document;
import nu.xom.Element;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.http.HttpHost;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.util.Collections.EMPTY_MAP;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.CONFLICT;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.NO_CONTENT;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.REQUEST_TIMEOUT;
import static nl.knaw.huygens.pergamon.janus.Identifier.requireValid;
import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static org.apache.http.entity.ContentType.APPLICATION_JSON;
import static org.elasticsearch.client.Requests.bulkRequest;
import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wrapperQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

/**
 * Backend that stores documents and annotations in an Elasticsearch cluster.
 */
public class ElasticBackend implements AutoCloseable {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticBackend.class);

  /**
   * Returns a Response with result as the entity. A null result becomes a 404, non-null a 200.
   * Intended to wrap the result of GETs.
   */
  public static Response asResponse(Object result) {
    return (result == null ? Response.status(NOT_FOUND) : Response.status(OK).entity(result)).build();
  }

  /**
   * Part of a paginated list.
   */
  public static class ListPage {
    @JsonProperty
    public List<String> result;

    @JsonProperty
    public int from;

    @JsonProperty
    public long total;

    static ListPage empty() {
      return new ListPage(0, 0, Collections.emptyList());
    }

    ListPage(int from, long total, List<String> result) {
      this.result = result;
      this.from = from;
      this.total = total;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ListPage listPage = (ListPage) o;
      return from == listPage.from &&
        total == listPage.total &&
        Objects.equals(result, listPage.result);
    }

    @Override
    public int hashCode() {
      return Objects.hash(result, from, total);
    }
  }

  private static final String ANNOTATION_INDEX = "janus_annotations";
  private static final String ANNOTATION_TYPE = "annotation";
  private static final String ANNOTATION_MAPPING_IN_JSON = "/annotation-mapping.json";

  private static ObjectMapper mapper = Jackson.newObjectMapper();

  // Name of ES index used to store annotations.
  private final String annotationIndex;
  // Type name of annotations in the ES index.
  private final String annotationType;

  private final Mapping mapping;

  final RestHighLevelClient hiClient;
  private final RestClient loClient;
  private final String documentIndex;
  private final String documentType;

  private final String fileStorageDir;

  private final String esSearchEndpoint;

  private final ReadWriteLock fsLock = new ReentrantReadWriteLock(); // protects filesystem operations

  /**
   * Construct Backend instance with a list of backing Elasticsearch connections.
   *
   * @param hosts         Hosts to connect to. These have the form addr:port,
   *                      where the port is optional and defaults to 9200.
   * @param documentIndex Name of the document index.
   * @param documentType  Name of the document type.
   * @throws UnknownHostException
   */
  public ElasticBackend(List<String> hosts, String documentIndex, String documentType, Mapping mapping,
                        String storageDir)
    throws UnknownHostException {
    this(hosts, documentIndex, documentType, ANNOTATION_INDEX, ANNOTATION_TYPE, mapping, storageDir);
  }

  // Arguments annotation{Index,Type} are for test purposes only.
  ElasticBackend(List<String> hosts, String documentIndex, String documentType,
                 String annotationIndex, String annotationType, Mapping mapping, String storageDir)
    throws UnknownHostException {
    if (Objects.equals(documentIndex, annotationIndex) || Objects.equals(annotationType, documentType)) {
      throw new IllegalArgumentException("documents shouldn't be stored in the annotation index");
    }

    if (hosts == null || hosts.isEmpty()) {
      hosts = Collections.singletonList("localhost:9200");
    }

    this.annotationIndex = annotationIndex;
    this.annotationType = annotationType;
    this.documentIndex = documentIndex;
    this.documentType = documentType;
    this.mapping = mapping;
    this.fileStorageDir = storageDir;

    loClient = RestClient.builder(hosts.stream()
                                       .map(ElasticBackend::parseAddr)
                                       .collect(Collectors.toList())
                                       .toArray(new HttpHost[hosts.size()]))
                         .build();

    hiClient = new RestHighLevelClient(loClient);

    esSearchEndpoint = String.format("%s/%s/_search", documentIndex, documentType);
  }

  // Parse address spec of the form <addr>[:<port>]
  static HttpHost parseAddr(String addr) {
    int port = 9200;

    int colon = addr.lastIndexOf(':');
    if (colon >= 0) {
      String after = addr.substring(colon + 1);
      if (!after.matches("[0-9:]+\\]")) {
        try {
          port = Integer.parseInt(after);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException(
            String.format("Invalid port number \"%s\"", escapeJava(after)), e);
        }
        addr = addr.substring(0, colon);
      }
    }

    return new HttpHost(addr, port);
  }

  private class EsClusterHealthCheck extends HealthCheck {
    private final boolean failOnYellow;

    EsClusterHealthCheck(boolean failOnYellow) {
      this.failOnYellow = failOnYellow;
    }

    @Override
    protected Result check() throws Exception {
      org.elasticsearch.client.Response r = loClient.performRequest("GET", "/_cluster/health");
      int httpstatus = r.getStatusLine().getStatusCode();
      if (!success(httpstatus)) {
        return Result.unhealthy(String.format("Got %d from %s /_cluster/health", httpstatus, r.getHost()));
      }
      Object status = Jackson.newObjectMapper()
                             .readValue(r.getEntity().getContent(), Map.class)
                             .get("status");
      if ("red".equals(status) || failOnYellow && "yellow".equals(status)) {
        return Result.unhealthy(String.format("ES cluster status = %s", status));
      }
      return Result.healthy();
    }
  }

  private class EsIndexExistsHealthCheck extends HealthCheck {
    @Override
    protected Result check() throws Exception {
      org.elasticsearch.client.Response r =
        loClient.performRequest("GET", String.format("%s/_stats", ANNOTATION_INDEX));
      int httpstatus = r.getStatusLine().getStatusCode();
      if (!success(httpstatus)) {
        return Result.unhealthy(String.format("Got %d when checking for index %s", httpstatus, ANNOTATION_INDEX));
      }
      return Result.healthy();
    }
  }

  public void registerHealthChecks(HealthCheckRegistry registry) {
    // These need to be rewritten to use the Elasticsearch REST client.
    registry.register("ES cluster health", new EsClusterHealthCheck(false));
    // registry.register("ES index docs health", new EsIndexDocsHealthCheck(client, documentIndex));
    registry.register("ES index exists health", new EsIndexExistsHealthCheck());
  }

  @Override
  public void close() throws Exception {
    loClient.close();
  }

  private boolean indexExists(String index) throws IOException {
    switch (loClient.performRequest("HEAD", index).getStatusLine().getStatusCode()) {
      case 200:
        return true;
      case 404:
        return false;
      default:
        // Shouldn't happen: ES client should throw a ResponseException
        throw new RuntimeException("unexpected error from Elasticsearch");
    }
  }

  public void initIndices() throws IOException {
    if (!indexExists(annotationIndex)) {
      loClient.performRequest("PUT", annotationIndex, Collections.emptyMap(),
        new InputStreamEntity(ElasticBackend.class.getResourceAsStream(ANNOTATION_MAPPING_IN_JSON)));
    }
    if (!indexExists(documentIndex)) {
      org.elasticsearch.client.Response r = loClient.performRequest("PUT", documentIndex,
        Collections.emptyMap(), new StringEntity(mapper.writeValueAsString(
          ImmutableMap.<String, Object>of(
            "mappings", ImmutableMap.<String, Object>of(
              documentType, mapping.asMap())))));
      int code = r.getStatusLine().getStatusCode();
      if (!success(code)) {
        throw new RuntimeException(String.format("creating document index: %d", code));
      }
    }
  }

  // For test purposes only.
  void removeIndices() throws IOException {
    loClient.performRequest("DELETE", "/" + annotationIndex);
    loClient.performRequest("DELETE", "/" + documentIndex);
  }

  /**
   * Reports information about the Elasticsearch cluster.
   */
  InputStream about() throws IOException {
    return loClient.performRequest("GET", "/").getEntity().getContent();
  }

  /**
   * Get the single annotation with the given id.
   */
  public Annotation getAnnotation(String id) {
    GetResponse response = get(annotationIndex, annotationType, id);
    if (!response.isExists()) {
      return null;
    }
    return makeAnnotation(response.getSourceAsMap(), id);
  }

  /**
   * Check if a document with the given id exists.
   */
  public boolean documentExists(String id) {
    return exists(documentIndex, documentType, id);
  }

  /**
   * Find the document with the given id.
   */
  public Optional<Map<String, Object>> findDocument(String id) {
    try {
      final GetResponse response = get(documentIndex, documentType, id);
      if (response.isSourceEmpty()) {
        return Optional.empty();
      }
      //return Optional.of((String) response.getSourceAsMap().get("body"));
      return Optional.of(response.getSourceAsMap());
    } catch (ElasticsearchStatusException e) {
      if (noSuchIndex(e)) {
        return Optional.empty();
      }
      throw e;
    }
  }

  /**
   * Retrieve the document with the given id and its annotations.
   * <p>
   * Returns null if no document has the given id.
   * <p>
   * If recursive, get annotations on annotations as well.
   */
  public DocAndAnnotations getWithAnnotations(String id, boolean recursive) {
    return findDocument(id).map(body -> addAnnotations(id, body, recursive))
                           .orElse(null);
  }

  private DocAndAnnotations addAnnotations(String id, Map<String, Object> body, boolean recursive) {
    final List<Annotation> annotations = getAnnotations(id, null, recursive, true, new ArrayList<>());
    return new DocAndAnnotations(id, body, annotations);
  }

  /**
   * Get annotations belong to id, optionally satisfying the query string q.
   * <p>
   * If recursive, get annotations on annotations as well.
   */
  public List<Annotation> getAnnotations(String id, @Nullable String q, boolean recursive) {
    return getAnnotations(id, q, recursive, false, new ArrayList<>());
  }

  // Fields of _source that we want below.
  private static final String[] ANNOTATION_FIELDS =
    new String[]{"attrib", "start", "end", "type", "source", "target", "body"};

  /*
   * Gets annotations on id, optionally filtered by query q.
   * If recursive, gets annotations on annotations etc.
   * If isRoot, id must be a document; we then use the root field for optimized fetching.
   */
  private List<Annotation> getAnnotations(String id, @Nullable String q, boolean recursive, boolean isRoot,
                                          List<Annotation> result) {
    BoolQueryBuilder query = boolQuery().filter(termQuery(recursive && isRoot ? "root" : "target", id));
    if (q != null) {
      query.must(queryStringQuery(q));
    }

    SearchResponse response;
    try {
      response = hiClient.search(searchRequest(annotationIndex)
        .types(annotationType)
        .source(searchSource().query(query)
                              .fetchSource(ANNOTATION_FIELDS, null)
                              .sort("order", SortOrder.ASC)
                              // TODO: should we scroll, or should the client scroll?
                              .size(1000)));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }

    List<Annotation> hits = Arrays.stream(response.getHits().getHits())
                                  .map(hit -> makeAnnotation(hit.getSourceAsMap(), hit.getId()))
                                  .collect(Collectors.toList());
    result.addAll(hits);

    // If id is a root (a document), searching for the "root" attribute
    // that caches its id suffices. Otherwise, we have to query recursively.
    if (recursive && !isRoot) {
      hits.forEach(ann -> getAnnotations(ann.id, q, true, false, result));
    }

    return result;
  }

  /**
   * List documents ids in index, with optional full-text search.
   *
   * @param query Query string (Lucene syntax). null to get all documents.
   * @return List of matching document ids.
   */
  public ListPage listDocs(@Nullable String query, int from, int count) {
    try {
      SearchResponse response = hiClient.search(
        searchRequest(documentIndex)
          .types(documentType)
          .source(searchSource().query(query == null ? matchAllQuery() : wrapperQuery(query))
                                .from(from)
                                .size(count)));

      return new ListPage(from, response.getHits().getTotalHits(),
        Arrays.stream(response.getHits().getHits())
              .map(SearchHit::getId)
              .collect(Collectors.toList()));
    } catch (ElasticsearchStatusException e) {
      if (noSuchIndex(e)) {
        return ListPage.empty();
      }
      throw e;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Annotation makeAnnotation(Map<String, Object> map, String id) {
    Annotation r = new Annotation((int) map.get("start"), (int) map.get("end"), (String) map.get("target"),
      (String) map.get("type"), (String) map.get("body"), (String) map.get("source"), id);
    copyAttributes(map, r);
    return r;
  }

  // prepareIndex + setOpType(CREATE)
  private IndexRequest prepareCreate(String index, String type, @Nullable String id) {
    return indexRequest(index).type(type).id(id);
  }

  /**
   * Stores the annotation ann, which must have its target set.
   */
  public PutResult putAnnotation(@Nullable String target, Annotation ann) throws IOException {
    if (ann.id != null) {
      return new PutResult(ann.id, BAD_REQUEST, "annotation may not determine its own id");
    }
    if (target != null && !Objects.equals(target, ann.target)) {
      return new PutResult(null, BAD_REQUEST,
        String.format("target mismatch: '%s' in path, '%s' in annotation", target, ann.target));
    }
    ann.target = target;
    return putAnnotation(ann);
  }

  PutResult putAnnotation(Annotation ann) throws IOException {
    try {
      // If there's a document with the id, we annotate that, else the annotation with the id.
      // XXX we need to be smarter, e.g., address the document by index/type/id.
      GetResponse got = get(documentIndex, documentType, ann.target);
      String root;
      if (got.isExists()) {
        root = ann.target;
      } else {
        got = get(annotationIndex, annotationType, ann.target);
        if (!got.isExists()) {
          return new PutResult(null, 404);
        }
        root = (String) got.getSourceAsMap().get("root");
      }

      IndexResponse response = hiClient.index(indexRequest(annotationIndex).type(annotationType).id(ann.id).source(
        jsonBuilder().startObject()
                     .field("start", ann.start)
                     .field("end", ann.end)
                     .field("attrib", ann.attributes)
                     .field("body", ann.body)
                     .field("type", ann.type)
                     .field("source", ann.source)
                     .field("target", ann.target)
                     .field("root", root)
                     .endObject()));
      return new PutResult(response.getId(), response.status().getStatus());
    } catch (VersionConflictEngineException e) {
      return new PutResult(null, CONFLICT, e.toString());
    } catch (Throwable e) {
      return errorResult(e);
    }
  }

  public Response addBody(String id, String bodyId) throws IOException {
    Annotation ann = getAnnotation(id);
    if (ann.body != null) {
      if (ann.body.equals(bodyId)) {
        return Response.status(NO_CONTENT).build();
      }
      return Response.status(CONFLICT).build();
    }
    return setBody(id, bodyId);
  }

  /**
   * Set body field of annotation to the id of a document.
   * Precondition (using the default addBody) is that annId exists and its body is null.
   */
  private Response setBody(String annId, String bodyId) throws IOException {
    // TODO Refactor. This is repeating the fetch on annId.
    GetResponse getR = get(annotationIndex, annotationType, annId);
    Map<String, Object> ann = getR.getSourceAsMap();

    GetResponse bodyR = get(documentIndex, documentType, bodyId);
    if (!bodyR.isExists()) {
      return Response.status(404).build();
    }
    ann.put("body", bodyId);

    IndexResponse idxR = hiClient.index(indexRequest(annotationIndex).type(annotationType).id(annId).source(ann));
    return Response.status(idxR.status().getStatus()).entity(ann).build();
  }

  private PutResult makePutResult(IndexResponse response) {
    int status = response.status().getStatus();
    String id = success(status) ? response.getId() : null;
    return new PutResult(id, status);
  }

  public PutResult putJSON(String id, String content) throws IOException {
    try {
      store(id, content);
    } catch (TimeoutException e) {
      return new PutResult(id, REQUEST_TIMEOUT);
    }

    // TODO parse and check if content has body field?
    try {
      IndexResponse response =
        hiClient.index(indexRequest(documentIndex).type(documentType).id(id).source(content, JSON));
      return makePutResult(response);
    } catch (ElasticsearchException e) {
      return new PutResult(id, e.status().getStatus(), e.toString());
    } catch (Throwable e) {
      return errorResult(e);
    }
  }

  public PutResult putTxt(String id, String content) throws IOException {
    if (id == null) {
      // XXX how to let Elasticsearch determine the id?
      // Without this step, we get:
      // ActionRequestValidationException: an id must be provided if version type or value are set
      id = UUID.randomUUID().toString();
    }
    try {
      store(id, content);
    } catch (FileAlreadyExistsException e) {
      return new PutResult(String.format("%s already exists in file store", id), 409);
    } catch (TimeoutException e) {
      return new PutResult(id, REQUEST_TIMEOUT);
    }
    try {
      IndexRequest req = indexRequest(documentIndex).type(documentType).id(id).create(true)
                                                    .source(jsonBuilder().startObject()
                                                                         .field("body", content)
                                                                         .endObject());
      return makePutResult(hiClient.index(req));
    } catch (ElasticsearchStatusException e) {
      return new PutResult(e.toString(), e.status().getStatus());
    }
  }

  /**
   * Store XML document's text with one annotation per XML element.
   */
  public PutResult putXml(String id, String document) throws IOException {
    if (id == null) {
      id = UUID.randomUUID().toString();
    }
    try {
      store(id, document);
    } catch (FileAlreadyExistsException e) {
      return new PutResult(id, CONFLICT);
    } catch (TimeoutException e) {
      return new PutResult(id, REQUEST_TIMEOUT);
    }
    try {
      Document xml = XmlParser.fromString(document);
      Triple<String, Element, Map<String, String>> fields = mapping.apply(xml);

      // first field is the special "body" field
      TaggedCodepoints body = new TaggedCodepoints(fields.getMiddle(), id);

      return putXml(fields.getLeft(), body, fields.getRight());
    } catch (Throwable e) {
      deleteFromStore(id);
      return new PutResult(id, BAD_REQUEST, e.toString());
    }
  }

  private PutResult putXml(String bodyField, TaggedCodepoints body, Map<String, String> fields) throws IOException {
    // TODO: handle partial failures better.
    String docId = body.docId;

    XContentBuilder doc = jsonBuilder().startObject()
                                       .field(bodyField, body.text());
    doc.field(bodyField, body.text());
    for (Map.Entry<String, String> entry : fields.entrySet()) {
      doc.field(entry.getKey(), entry.getValue());
    }
    doc.endObject();

    IndexResponse response = hiClient.index(indexRequest(documentIndex)
      .type(documentType).id(docId).create(true)
      .source(doc));
    int status = response.status().getStatus();
    if (!success(status)) {
      return new PutResult(null, status);
    }

    List<Annotation> tags = body.tags();
    BulkRequest bulk = bulkRequest();
    for (int i = 0; i < tags.size(); i++) {
      Annotation ann = tags.get(i);
      bulk.add(indexRequest(annotationIndex).type(annotationType).id(ann.id)
                                            .create(true)
                                            .source(jsonBuilder()
                                              .startObject()
                                              .field("start", ann.start)
                                              .field("end", ann.end)
                                              .field("attrib", ann.attributes)
                                              .field("type", ann.type)
                                              .field("source", "xml")
                                              .field("target", ann.target)
                                              .field("root", docId)
                                              // The order field is only used to sort, so that we get XML tags back
                                              // in exactly the order they appeared in the original.
                                              // XXX do we need this?
                                              .field("order", i)
                                              .endObject()
                                            ));
    }

    if (bulk.numberOfActions() > 0) {
      for (BulkItemResponse item : hiClient.bulk(bulk)) {
        status = item.status().getStatus();
        if (!success(status)) {
          return new PutResult(docId, status);
        }
      }
    }
    return new PutResult(docId, 201);
  }

  /**
   * Deletes a document and all annotations pointing to it (directly or indirectly).
   */
  public RestStatus delete(String id) throws IOException {
    boolean fileFound = true;
    if (fileStorageDir != null) {
      try {
        deleteFromStore(id);
      } catch (NoSuchFileException e) {
        fileFound = false;
      }
    }

    org.elasticsearch.client.Response annR =
      loClient.performRequest("POST", String.format("%s/%s/_delete_by_query", annotationIndex, annotationType),
        Collections.emptyMap(),
        new StringEntity(
          mapper.writeValueAsString(
            ImmutableMap.of("query", ImmutableMap.of("term", ImmutableMap.of("root", id))))));
    if (annR.getStatusLine().getStatusCode() != 200) {
      LOG.warn("Got {} when deleting annotation for {}", annR.getStatusLine().getStatusCode(), id);
    }

    DeleteResponse docR =
      hiClient.delete(new DeleteRequest(documentIndex, documentType, id));
    if (docR.status() == RestStatus.OK && !fileFound) {
      LOG.warn("{} was in Elasticsearch but not in the file store", id);
    }
    return docR.status();
  }

  public PutResult updateXml(String id, String content) throws IOException {
    if (id == null) {
      throw new NullPointerException("id should not be null");
    }
    Optional<byte[]> orig = null;
    try {
      orig = getOriginalBytes(id);
    } catch (TimeoutException e) {
      return new PutResult(id, REQUEST_TIMEOUT);
    }
    if (orig.isPresent() && content.equals(new String(orig.get()))) {
      return new PutResult(id, 200);
    }
    try {
      delete(id);
    } catch (NoSuchFileException e) {
    }
    return putXml(id, content);
  }

  private Path getOriginalPath(String id) {
    return Paths.get(fileStorageDir, id);
  }

  private void store(String id, String content) throws IOException, TimeoutException {
    if (fileStorageDir == null) {
      return;
    }
    requireValid(id);
    Path path = getOriginalPath(id);

    writeLock(id);

    try (BufferedWriter out = Files.newBufferedWriter(path, CREATE_NEW)) {
      out.write(content);
    } catch (Throwable e) {
      Files.delete(path);
      throw e;
    } finally {
      writeUnlock(id);
    }
  }

  private void deleteFromStore(String id) throws IOException {
    if (fileStorageDir == null) {
      return;
    }
    Files.delete(getOriginalPath(id));
  }

  public Optional<byte[]> getOriginalBytes(String id) throws TimeoutException {
    readLock(id);

    try {
      return Optional.of(Files.readAllBytes(getOriginalPath(id)));
    } catch (IOException e) {
      LOG.warn("Failed to get original {}: {}", id, e.getMessage());
      return Optional.empty();
    } finally {
      readUnlock(id);
    }
  }

  public String getOriginal(String id) throws IOException, TimeoutException {
    Path path = getOriginalPath(id);
    readLock(id);

    try {
      return new String(Files.readAllBytes(path));
    } finally {
      readUnlock(id);
    }
  }

  @SuppressWarnings("unchecked")
  private static void copyAttributes(Map<String, Object> map, Annotation ann) {
    ((Map<String, String>) map.getOrDefault("attrib", EMPTY_MAP)).forEach(ann.attributes::put);
  }

  /**
   * Pass query to Elasticsearch.
   */
  public org.elasticsearch.client.Response search(String query) throws IOException {
    org.elasticsearch.client.Response r;
    try {
      final StringEntity entity = new StringEntity(query, APPLICATION_JSON);
      r = loClient.performRequest("GET", esSearchEndpoint, Collections.emptyMap(), entity);
    } catch (ResponseException e) {
      r = e.getResponse();
    }
    return r;
  }

  // Painless script to find co-occurrences of two fields' values.
  // Because of return type limitations, we have to return a single string
  // to represent a pair. The SEPARATOR is chosen to be as unlikely as possible.
  // It must not contain a %, since it will be passed through String.format.
  private static final String SEPARATOR = "$$\t\t^^__";
  private static final String PAIR_QUERY =
    "  def s = doc['%s'].value;" +
      "def r = doc['%s'].value;" +
      "if (s.compareTo(r) < 0) { s + '" + SEPARATOR + "' + r } else { r + '" + SEPARATOR + "' + s }";

  /**
   * Co-occurrence graph of two field in documents that pass a filter.
   *
   * @param filter
   * @return
   * @throws IOException
   */
  public List<Map<String, Object>> cooccurrence(Object filter, String field1, String field2) throws IOException {
    // Validate fields to prevent nasty queries from passing through
    validateFieldName(field1);
    validateFieldName(field2);

    // XXX this is wasteful, but we need to pass the filter query as a string to the ES client.
    // Can the Resource method require JSON without parsing it?
    ObjectMapper mapper = Jackson.newObjectMapper();
    String filterExpr = mapper.writeValueAsString(filter);

    SearchRequest request = new SearchRequest(documentIndex)
      .source(
        new SearchSourceBuilder()
          .query(QueryBuilders.boolQuery().filter(wrapperQuery(filterExpr)))
          .size(0)
          .aggregation(AggregationBuilders.terms("pairs")
                                          .script(new Script(String.format(PAIR_QUERY, field1, field2)))
                                          .size(10000))
      );

    Terms pairs = (Terms) hiClient.search(request).getAggregations().asMap().get("pairs");
    return pairs.getBuckets().stream()
                .map(bucket -> {
                  String places = (String) bucket.getKey();
                  int sep = places.indexOf(SEPARATOR);
                  String src = places.substring(0, sep);
                  String tgt = places.substring(sep + SEPARATOR.length(), places.length());
                  return ImmutableMap.<String, Object>of(
                    "weight", bucket.getDocCount(),
                    "source", src,
                    "target", tgt);
                })
                .collect(Collectors.toList());
  }

  private static void validateFieldName(String field) {
    if (!Pattern.matches("[a-z0-9_-]+", field)) {
      throw new RuntimeException("invalid field name '" + field + "'");
    }
  }

  // TODO: we need more fine-grained locks.

  private void readLock(String id) throws TimeoutException {
    if (!fsLock.readLock().tryLock()) {
      throw new TimeoutException("could not get lock for " + id);
    }
  }

  private void writeLock(String id) throws TimeoutException {
    if (!fsLock.writeLock().tryLock()) {
      throw new TimeoutException("could not get lock for " + id);
    }
  }

  private void readUnlock(String id) {
    fsLock.readLock().unlock();
  }

  private void writeUnlock(String id) {
    fsLock.writeLock().unlock();
  }

  private PutResult errorResult(Throwable e) {
    if ("error while performing request".equals(e.getMessage())) {
      e = e.getCause();
    }
    return new PutResult(null, 500, e.toString());
  }

  private GetResponse get(String index, String type, String id) {
    try {
      return hiClient.get(getRequest(index).type(type).id(id));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean exists(String index, String type, String id) {
    try {
      return hiClient.exists(getRequest(index).type(type).id(id));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean noSuchIndex(ElasticsearchStatusException e) {
    return e.status() == RestStatus.NOT_FOUND && e.getMessage().contains("no such index");
  }

  private static boolean success(int status) {
    return status >= 200 && status < 300;
  }

  /**
   * Returned by PUT/POST methods.
   */
  class PutResult {
    /**
     * Id of document or annotation that was created.
     */
    @JsonProperty
    public final String id;

    /**
     * HTTP status code.
     */
    @JsonProperty
    public final int status;

    /**
     * Optional error message.
     */
    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    public final String message;

    PutResult(String id, int status, String message) {
      this.id = id;
      this.status = status;
      this.message = message;
    }

    PutResult(String id, int status) {
      this(id, status, null);
    }

    PutResult(String id, Response.Status status) {
      this(id, status, null);
    }

    PutResult(String id, Response.Status status, String message) {
      this(id, status.getStatusCode(), message);
    }

    Response asResponse() {
      return Response.status(status).entity(this).build();
    }
  }
}
