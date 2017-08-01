package nl.knaw.huygens.pergamon.janus;

import com.google.common.io.CharStreams;
import nl.knaw.huygens.pergamon.janus.xml.Tag;
import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Collections.EMPTY_MAP;
import static org.apache.commons.lang3.StringEscapeUtils.escapeJava;
import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class ElasticBackend implements Backend {
  // Name of ES index used to store annotations.
  private final String annotationIndex;
  // Type name of annotations in the ES index.
  private final String annotationType;

  private final Client client;
  private final String documentIndex;
  private final String documentType;

  /**
   * @param documentIndex Name of the document index.
   * @param documentType  Name of the document type.
   * @throws UnknownHostException
   */
  public ElasticBackend(List<String> hosts, String documentIndex, String documentType) throws UnknownHostException {
    this(hosts, documentIndex, documentType, "janus_annotations", "annotation");
  }

  // Final two arguments are for test purposes only.
  ElasticBackend(List<String> hosts, String documentIndex, String documentType,
                 String annotationIndex, String annotationType) throws UnknownHostException {
    if (Objects.equals(documentIndex, annotationIndex) || Objects.equals(annotationType, documentType)) {
      throw new IllegalArgumentException("documents shouldn't be stored in the annotation index");
    }

    this.annotationIndex = annotationIndex;
    this.annotationType = annotationType;
    this.documentIndex = documentIndex;
    this.documentType = documentType;

    TransportClient cli = new PreBuiltTransportClient(Settings.EMPTY);
    if (hosts.isEmpty()) {
      cli.addTransportAddress(new InetSocketTransportAddress(new InetSocketAddress("localhost", 9300)));
    }
    for (String host : hosts) {
      cli.addTransportAddress(parseAddr(host));
    }

    client = cli;
  }

  static InetSocketTransportAddress parseAddr(String addr) throws UnknownHostException {
    int port = 9300;

    int colon = addr.lastIndexOf(':');
    if (colon >= 0) {
      try {
        port = Integer.parseInt(addr.substring(colon + 1));
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException(
          String.format("Invalid port number \"%s\"", escapeJava(addr.substring(colon + 1))), e);
      }
      addr = addr.substring(0, colon);
    }

    return new InetSocketTransportAddress(new InetSocketAddress(addr, port));
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  // For test purposes only.
  boolean initIndices() throws IOException {
    InputStream mapping = ElasticBackend.class.getResourceAsStream("/annotation-mapping.json");
    CreateIndexResponse resp = client.admin().indices().prepareCreate(annotationIndex)
                                     .addMapping(annotationType, CharStreams.toString(new InputStreamReader(mapping)),
                                       XContentType.JSON)
                                     .get();
    if (!resp.isAcknowledged()) {
      return false;
    }
    return client.admin().indices().prepareCreate(documentIndex).get().isAcknowledged();
  }

  // For test purposes only.
  void removeIndices() {
    client.admin().indices().prepareDelete(annotationIndex, documentIndex).get();
  }

  @Override
  public Annotation getAnnotation(String id) {
    GetResponse response = client.prepareGet(annotationIndex, annotationType, id).get();
    return makeAnnotation(response.getSourceAsMap(), id);
  }

  @Override
  public DocAndAnnotations getWithAnnotations(String id, boolean recursive) throws IOException {
    boolean isRoot = true;
    GetResponse response = client.prepareGet(documentIndex, documentType, id).get();
    if (!response.isExists()) {
      response = client.prepareGet(annotationIndex, annotationType, id).get();
      if (!response.isExists()) {
        return null;
      }
      isRoot = false;
    }

    return new DocAndAnnotations((String) response.getSourceAsMap().get("body"),
      getAnnotations(id, null, recursive, isRoot, new ArrayList<>()));
  }

  @Override
  public List<Annotation> getAnnotations(String id, @Nullable String q, boolean recursive) {
    return getAnnotations(id, q, recursive, false, new ArrayList<>());
  }

  // Fields of _source that we want below.
  private static final String[] ANNOTATION_FIELDS = new String[]{"attrib", "start", "end", "tag", "type", "target"};

  private List<Annotation> getAnnotations(String id, @Nullable String q, boolean recursive, boolean isRoot,
                                          List<Annotation> result) {
    BoolQueryBuilder query = boolQuery().filter(termQuery(recursive && isRoot ? "root" : "target", id));
    if (q != null) {
      query.must(queryStringQuery(q));
    }

    SearchResponse response = client.prepareSearch(annotationIndex)
                                    .setTypes(annotationType)
                                    .setQuery(query)
                                    .setFetchSource(ANNOTATION_FIELDS, null)
                                    .addSort("order", SortOrder.ASC)
                                    // TODO: should we scroll, or should the client scroll?
                                    .setSize(1000).get();

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
  public List<String> listDocuments() {
    SearchResponse response = client.prepareSearch(documentIndex)
                                    .setTypes(documentType)
                                    .setQuery(matchAllQuery())
                                    .setFetchSource(false)
                                    .setSize(10000).get(); // TODO pagination
    return Arrays.stream(response.getHits().getHits())
                 .map(SearchHit::getId)
                 .collect(Collectors.toList());
  }

  private static Annotation makeAnnotation(Map<String, Object> map, String id) {
    Annotation r = new Annotation((int) map.get("start"), (int) map.get("end"), (String) map.get("target"),
      (String) map.get("tag"), null, (String) map.get("type"), id);
    copyAttributes(map, r);
    return r;
  }

  // prepareIndex + setOpType(CREATE)
  private IndexRequestBuilder prepareCreate(String index, String type, @Nullable String id) {
    IndexRequestBuilder request = client.prepareIndex(index, type, id);
    if (id != null) {
      // the create op explicitly requires an id to be set ... If you want to have
      // automatic ID generation you need to use the "index" operation.
      // https://github.com/elastic/elasticsearch/issues/21535#issuecomment-260467699
      request.setOpType(CREATE);
    }
    return request;
  }

  @Override
  public PutResult putAnnotation(Annotation ann) throws IOException {
    try {
      // If there's a document with the id, we annotate that, else the annotation with the id.
      // XXX we need to be smarter, e.g., address the document by index/type/id.
      GetResponse got = client.prepareGet(documentIndex, documentType, ann.target).get();
      String root;
      if (got.isExists()) {
        root = ann.target;
      } else {
        got = client.prepareGet(annotationIndex, annotationType, ann.target).get();
        if (!got.isExists()) {
          return new PutResult(null, 404);
        }
        root = (String) got.getSourceAsMap().get("root");
      }

      IndexResponse response = prepareCreate(annotationIndex, annotationType, ann.id).setSource(
        jsonBuilder().startObject()
                     .field("start", ann.start)
                     .field("end", ann.end)
                     .field("attrib", ann.attributes)
                     .field("body", ann.body)
                     .field("tag", ann.tag)
                     // XXX hard-wire type to something like "user"?
                     .field("type", ann.type)
                     .field("target", ann.target)
                     .field("root", root)
                     .endObject()
      ).get();
      return new PutResult(response.getId(), response.status().getStatus());
    } catch (VersionConflictEngineException e) {
      return new PutResult(e.toString(), Response.Status.CONFLICT);
    }
  }

  @Override
  public PutResult putTxt(String id, String content) throws IOException {
    try {
      IndexResponse response = prepareCreate(documentIndex, documentType, id).setSource(
        jsonBuilder().startObject()
                     .field("body", content)
                     .endObject()
      ).get();
      int status = response.status().getStatus();
      if (status < 200 || status >= 300) {
        id = null;
      } else {
        id = response.getId();
      }
      return new PutResult(id, status);
    } catch (VersionConflictEngineException e) {
      return new PutResult(e.toString(), Response.Status.CONFLICT);
    }
  }

  @Override
  public PutResult putXml(TaggedCodepoints document) throws IOException {
    // TODO: handle partial failures better.
    try {
      String docId = document.docId;
      IndexResponse response = prepareCreate(documentIndex, documentType, docId).setSource(
        jsonBuilder().startObject()
                     .field("body", document.text())
                     .endObject()
      ).get();
      int status = response.status().getStatus();
      if (status < 200 || status >= 300) {
        return new PutResult(null, status);
      }

      List<Tag> tags = document.tags();
      BulkRequestBuilder bulk = client.prepareBulk();
      for (int i = 0; i < tags.size(); i++) {
        Tag ann = tags.get(i);
        bulk.add(prepareCreate(annotationIndex, annotationType, ann.id)
          .setSource(jsonBuilder()
            .startObject()
            .field("start", ann.start)
            .field("end", ann.end)
            .field("attrib", ann.attributes)
            .field("tag", ann.tag)
            .field("type", "tag")
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

      BulkItemResponse[] items = bulk.get().getItems();
      for (BulkItemResponse item : items) {
        status = item.status().getStatus();
        if (status < 200 || status >= 300) {
          return new PutResult(docId, status);
        }
      }
      return new PutResult(docId, 201);
    } catch (VersionConflictEngineException e) {
      return new PutResult(e.toString(), Response.Status.CONFLICT);
    }
  }

  @Override
  public String getXml(String id) throws IOException {
    GetResponse response = client.prepareGet(documentIndex, documentType, id).get();
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
    new String[]{"attrib", "start", "end", "tag", "target", "xmlParent"};

  private List<Tag> getTags(String id) {
    BoolQueryBuilder query = boolQuery().filter(termQuery("target", id))
                                        .filter(termQuery("type", "tag"))
                                        .filter(existsQuery("xmlParent"));

    SearchResponse response = client.prepareSearch(annotationIndex)
                                    .setTypes(annotationType)
                                    .setQuery(query)
                                    .setFetchSource(TAG_FIELDS, null)
                                    .addSort("order", SortOrder.ASC)
                                    // TODO: we should scroll.
                                    .setSize(1000).get();

    return Arrays.stream(response.getHits().getHits()).map(hit -> {
      Map<String, Object> map = hit.getSourceAsMap();

      Tag tag = new Tag(hit.getId(), (String) map.get("tag"), (int) map.get("start"), (int) map.get("end"),
        (String) map.get("target"), (String) map.get("xmlParent"));
      copyAttributes(map, tag);

      return tag;
    }).collect(Collectors.toList());
  }

  @SuppressWarnings("unchecked")
  private static void copyAttributes(Map<String, Object> map, Annotation ann) {
    ((Map<String, String>) map.getOrDefault("attrib", EMPTY_MAP)).forEach(ann.attributes::put);
  }
}
