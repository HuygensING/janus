package nl.knaw.huygens.pergamon.janus;

import com.codahale.metrics.health.HealthCheckRegistry;
import io.dropwizard.jackson.Jackson;
import nl.knaw.huygens.pergamon.janus.xml.Tag;
import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
import org.apache.http.HttpHost;
import org.apache.http.entity.InputStreamEntity;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static java.util.Collections.EMPTY_MAP;
import static org.apache.commons.text.StringEscapeUtils.escapeJava;
import static org.elasticsearch.client.Requests.bulkRequest;
import static org.elasticsearch.client.Requests.getRequest;
import static org.elasticsearch.client.Requests.indexRequest;
import static org.elasticsearch.client.Requests.searchRequest;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.common.xcontent.XContentType.JSON;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.index.query.QueryBuilders.wrapperQuery;
import static org.elasticsearch.search.builder.SearchSourceBuilder.searchSource;

/**
 * Backend that stores documents and annotations in an Elasticsearch cluster.
 */
public class ElasticBackend implements Backend {
  private static final String ANNOTATION_INDEX = "janus_annotations";
  private static final String ANNOTATION_TYPE = "annotation";
  private static final String ANNOTATION_MAPPING_IN_JSON = "/annotation-mapping.json";

  // Name of ES index used to store annotations.
  private final String annotationIndex;
  // Type name of annotations in the ES index.
  private final String annotationType;

  private final RestHighLevelClient hiClient;
  private final RestClient loClient;
  final String documentIndex;
  final String documentType;

  /**
   * Construct Backend instance with a list of backing Elasticsearch connections.
   *
   * @param hosts         Hosts to connect to. These have the form addr:port,
   *                      where the port is optional and defaults to 9200.
   * @param documentIndex Name of the document index.
   * @param documentType  Name of the document type.
   * @throws UnknownHostException
   */
  public ElasticBackend(List<String> hosts, String documentIndex, String documentType) throws UnknownHostException {
    this(hosts, documentIndex, documentType, ANNOTATION_INDEX, ANNOTATION_TYPE);
  }

  // Final two arguments are for test purposes only.
  ElasticBackend(List<String> hosts, String documentIndex, String documentType,
                 String annotationIndex, String annotationType) throws UnknownHostException {
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

    loClient = RestClient.builder(hosts.stream()
                                       .map(ElasticBackend::parseAddr)
                                       .collect(Collectors.toList())
                                       .toArray(new HttpHost[hosts.size()]))
                         .build();

    hiClient = new RestHighLevelClient(loClient);
  }

  // Parse address spec of the form <addr>[:<port>]
  static HttpHost parseAddr(String addr) {
    int port = 9300;

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

  public void registerHealthChecks(HealthCheckRegistry registry) {
    // These need to be rewritten to use the Elasticsearch REST client.
    // registry.register("ES cluster health", new EsClusterHealthCheck(client));
    // registry.register("ES index docs health", new EsIndexDocsHealthCheck(client, documentIndex));
    // registry.register("ES index exists health", new EsIndexExistsHealthCheck(client, ANNOTATION_INDEX));
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

  void initIndices() throws IOException {
    if (!indexExists(annotationIndex)) {
      loClient.performRequest("PUT", annotationIndex, Collections.emptyMap(),
        new InputStreamEntity(ElasticBackend.class.getResourceAsStream(ANNOTATION_MAPPING_IN_JSON)));
    }
    if (!indexExists(documentIndex)) {
      org.elasticsearch.client.Response r = loClient.performRequest("PUT", documentIndex);
    }
  }

  // For test purposes only.
  void removeIndices() throws IOException {
    loClient.performRequest("DELETE", "/" + annotationIndex);
    loClient.performRequest("DELETE", "/" + documentIndex);
  }

  @Override
  public Annotation getAnnotation(String id) {
    GetResponse response = get(annotationIndex, annotationType, id);
    if (!response.isExists()) {
      return null;
    }
    return makeAnnotation(response.getSourceAsMap(), id);
  }

  @Override
  public DocAndAnnotations getWithAnnotations(String id, boolean recursive) {
    try {
      GetResponse response = get(documentIndex, documentType, id);
      if (response.isSourceEmpty()) {
        return null;
      }
      return new DocAndAnnotations(id, (String) response.getSourceAsMap().get("body"),
        getAnnotations(id, null, recursive, true, new ArrayList<>()));
    } catch (ElasticsearchStatusException e) {
      if (noSuchIndex(e)) {
        return null;
      }
      throw e;
    }
  }

  @Override
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

  @Override
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

  @Override
  public PutResult putAnnotation(Annotation ann) throws IOException {
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
      return new PutResult(e.toString(), Response.Status.CONFLICT);
    }
  }

  @Override
  public Response setBody(String annId, String bodyId) throws IOException {
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
    String id = (status < 200 || status >= 300) ? null : response.getId();
    return new PutResult(id, status);
  }

  @Override
  public PutResult putJSON(String id, String content) throws IOException {
    // TODO parse and check if content has body field?
    try {
      IndexResponse response =
        hiClient.index(indexRequest(documentIndex).type(documentType).id(id).source(content, JSON));
      return makePutResult(response);
    } catch (VersionConflictEngineException e) {
      return new PutResult(e.toString(), Response.Status.CONFLICT);
    }
  }

  @Override
  public PutResult putTxt(String id, String content) throws IOException {
    try {
      if (id == null) {
        // XXX how to let Elasticsearch determine the id?
        // Without this step, we get:
        // ActionRequestValidationException: an id must be provided if version type or value are set
        id = UUID.randomUUID().toString();
      }
      IndexRequest req = indexRequest(documentIndex).type(documentType).id(id).create(true)
                                                    .source(jsonBuilder().startObject()
                                                                         .field("body", content)
                                                                         .endObject());
      return makePutResult(hiClient.index(req));
    } catch (ElasticsearchStatusException e) {
      return new PutResult(e.toString(), e.status().getStatus());
    }
  }

  @Override
  public PutResult putXml(TaggedCodepoints document) throws IOException {
    // TODO: handle partial failures better.
    String docId = document.docId;
    IndexResponse response = hiClient.index(indexRequest(documentIndex)
      .type(documentType).id(docId).create(true)
      .source(jsonBuilder().startObject()
                           .field("body", document.text())
                           .endObject()));
    int status = response.status().getStatus();
    if (status < 200 || status >= 300) {
      return new PutResult(null, status);
    }

    List<Tag> tags = document.tags();
    BulkRequest bulk = bulkRequest();
    for (int i = 0; i < tags.size(); i++) {
      Tag ann = tags.get(i);
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
                                              .field("xmlParent", ann.xmlParent)
                                              .endObject()
                                            ));
    }

    if (bulk.numberOfActions() > 0) {
      for (BulkItemResponse item : hiClient.bulk(bulk)) {
        status = item.status().getStatus();
        if (status < 200 || status >= 300) {
          return new PutResult(docId, status);
        }
      }
    }
    return new PutResult(docId, 201);
  }

  @Override
  public String getXml(String id) throws IOException {
    GetResponse response = hiClient.get(new GetRequest(documentIndex, documentType, id));
    if (!response.isExists()) {
      return null;
    }
    String text = (String) response.getSourceAsMap().get("body");
    List<Tag> tags = getTags(id);
    if (tags.isEmpty()) {
      // TODO turn this into a response with the right status code (bad request?)
      throw new IllegalArgumentException("not originally an XML document");
    }

    return new TaggedCodepoints(text, id, tags).reconstruct().toXML();
  }

  private static final String[] TAG_FIELDS =
    new String[]{"attrib", "start", "end", "type", "target", "xmlParent"};

  private List<Tag> getTags(String id) throws IOException {
    BoolQueryBuilder query = boolQuery().filter(termQuery("target", id))
                                        .filter(termQuery("source", "xml"))
                                        .filter(existsQuery("xmlParent"));

    SearchResponse response = hiClient.search(
      new SearchRequest(annotationIndex)
        .source(searchSource().query(query)
                              .fetchSource(TAG_FIELDS, null)
                              .sort("order", SortOrder.ASC)
                              // TODO: we should scroll.
                              .size(1000)
        )
        .types(annotationType));

    return Arrays.stream(response.getHits().getHits()).map(hit -> {
      Map<String, Object> map = hit.getSourceAsMap();

      Tag tag = new Tag(hit.getId(), (String) map.get("type"), (int) map.get("start"), (int) map.get("end"),
        (String) map.get("target"), (String) map.get("xmlParent"));
      copyAttributes(map, tag);

      return tag;
    }).collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static void copyAttributes(Map<String, Object> map, Annotation ann) {
    ((Map<String, String>) map.getOrDefault("attrib", EMPTY_MAP)).forEach(ann.attributes::put);
  }

  /**
   * Pass query to Elasticsearch.
   */
  public SearchResponse search(String query) throws IOException {
    Map q;
    try {
      q = Jackson.newObjectMapper().readValue(query, Map.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return hiClient.search(searchRequest(documentIndex).source(searchSource().query(wrapperQuery(query))));
  }

  private GetResponse get(String index, String type, String id) {
    try {
      return hiClient.get(getRequest(index).type(type).id(id));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private boolean noSuchIndex(ElasticsearchStatusException e) {
    return e.status() == RestStatus.NOT_FOUND && e.getMessage().contains("no such index");
  }
}
