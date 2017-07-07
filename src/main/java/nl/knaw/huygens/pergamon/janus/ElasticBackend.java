package nl.knaw.huygens.pergamon.janus;

import nl.knaw.huygens.pergamon.janus.xml.Tag;
import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Collections.EMPTY_MAP;
import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;
import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class ElasticBackend implements Backend {
  // Name of ES index used to store annotations.
  private static final String ANNOTATION_INDEX = "janus_annotations";
  // Type name of annotations in the ES index.
  private static final java.lang.String ANNOTATION_TYPE = "annotation";

  private final Client client;
  private final String documentIndex;
  private final String documentType;

  /**
   * @param documentIndex Name of the document index.
   * @param documentType  Name of the document type.
   * @throws UnknownHostException
   */
  public ElasticBackend(String documentIndex, String documentType) throws UnknownHostException {
    if (ANNOTATION_INDEX.equals(documentIndex) && ANNOTATION_TYPE.equals(documentType)) {
      throw new IllegalArgumentException("documents shouldn't be stored in the annotation index");
    }

    client = new PreBuiltTransportClient(Settings.EMPTY)
      .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("localhost"), 9300));
    this.documentIndex = documentIndex;
    this.documentType = documentType;
  }

  @Override
  public void close() throws Exception {
    client.close();
  }

  @Override
  public Map<String, Object> getWithAnnotations(String id, boolean recursive) throws IOException {
    boolean isRoot = true;
    GetResponse response = client.prepareGet(documentIndex, documentType, id).get();
    if (!response.isExists()) {
      response = client.prepareGet(ANNOTATION_INDEX, ANNOTATION_TYPE, id).get();
      if (!response.isExists()) {
        return null;
      }
      isRoot = false;
    }

    HashMap<String, Object> result = new HashMap<>();
    result.put("annotations", getAnnotations(id, null, recursive, isRoot, new ArrayList<>()));
    Object text = response.getSourceAsMap().get("body");
    if (text != null) {
      result.put("text", text);
    }
    return result;
  }

  @Override
  public List<Object> getAnnotations(String id, @Nullable String q, boolean recursive) {
    return getAnnotations(id, q, recursive, false, new ArrayList<>());
  }

  // Fields of _source that we want below.
  private static final String[] ANNOTATION_FIELDS = new String[]{"attrib", "start", "end", "tag", "type"};

  private List<Object> getAnnotations(String id, @Nullable String q, boolean recursive, boolean isRoot,
                                      List<Object> result) {
    BoolQueryBuilder query = boolQuery().filter(termQuery(recursive && isRoot ? "root" : "target", id));
    if (q != null) {
      query.must(queryStringQuery(q));
    }

    SearchResponse response = client.prepareSearch(ANNOTATION_INDEX)
                                    .setTypes(ANNOTATION_TYPE)
                                    .setQuery(query)
                                    .setFetchSource(ANNOTATION_FIELDS, null)
                                    // TODO: should we scroll, or should the client scroll?
                                    .setSize(1000).get();

    List<Map<String, Object>> hits = Arrays.stream(response.getHits().getHits()).map(hit -> {
      String hitId = hit.getId();
      Map<String, Object> map = hit.getSourceAsMap();

      // Make returned object smaller on the wire.
      if (((Map<?, ?>) map.getOrDefault("attrib", EMPTY_MAP)).isEmpty()) {
        map.remove("attrib");
      }

      map.put("id", hitId);
      return map;
    }).collect(Collectors.toList());
    result.addAll(hits);

    // If id is a root (a document), searching for the "root" attribute
    // that caches its id suffices. Otherwise, we have to query recursively.
    if (recursive && !isRoot) {
      hits.forEach(map -> getAnnotations((String) map.get("id"), q, true, false, result));
    }

    return result;
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
  public PutResponse putAnnotation(Annotation ann, String id, String target) throws IOException {
    try {
      // If there's a document with the id, we annotate that, else the annotation with the id.
      // XXX we need to be smarter, e.g., address the document by index/type/id.
      GetResponse got = client.prepareGet(documentIndex, documentType, target).get();
      String root;
      if (got.isExists()) {
        root = target;
      } else {
        got = client.prepareGet(ANNOTATION_INDEX, ANNOTATION_TYPE, target).get();
        if (!got.isExists()) {
          return new PutResponse(null, 404);
        }
        root = (String) got.getSourceAsMap().get("root");
      }

      IndexResponse response = prepareCreate(ANNOTATION_INDEX, ANNOTATION_TYPE, id).setSource(
        jsonBuilder().startObject()
                     .field("start", ann.start)
                     .field("end", ann.end)
                     .field("attrib", ann.attributes)
                     .field("body", ann.body)
                     // XXX hard-wire type to something like "user"?
                     .field("type", ann.type)
                     .field("target", target)
                     .field("root", root)
                     .endObject()
      ).get();
      return new PutResponse(response.getId(), response.status().getStatus());
    } catch (VersionConflictEngineException e) {
      return new PutResponse(e.toString(), Response.Status.CONFLICT);
    }
  }

  @Override
  public PutResponse putTxt(String id, String content) throws IOException {
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
      return new PutResponse(id, status);
    } catch (VersionConflictEngineException e) {
      return new PutResponse(e.toString(), Response.Status.CONFLICT);
    }
  }

  @Override
  public PutResponse putXml(String id, TaggedCodepoints document) throws IOException {
    try {
      IndexResponse response = prepareCreate(documentIndex, documentType, id).setSource(
        jsonBuilder().startObject()
                     .field("body", document.text())
                     .endObject()
      ).get();
      int status = response.status().getStatus();
      if (status < 200 || status >= 300) {
        return new PutResponse(null, status);
      }
      id = response.getId();

      List<Tag> tags = document.tags();
      BulkRequestBuilder bulk = client.prepareBulk();
      for (int i = 0; i < tags.size(); i++) {
        Tag ann = tags.get(i);
        bulk.add(client.prepareIndex(ANNOTATION_INDEX, ANNOTATION_TYPE)
                       .setSource(jsonBuilder()
                         .startObject()
                         .field("start", ann.start)
                         .field("end", ann.end)
                         .field("attrib", ann.attributes)
                         .field("tag", ann.tag)
                         .field("type", "tag")
                         .field("target", id)
                         .field("root", id)
                         // The order field is only used to sort, so that we get XML tags back
                         // in exactly the order they appeared in the original.
                         // XXX do we need this?
                         .field("order", i)
                         .endObject()
                       ));
      }

      BulkItemResponse[] items = bulk.get().getItems();
      for (BulkItemResponse item : items) {
        status = item.status().getStatus();
        if (status < 200 || status >= 300) {
          return new PutResponse(id, status);
        }
      }
      return new PutResponse(id, 200);
    } catch (VersionConflictEngineException e) {
      return new PutResponse(e.toString(), Response.Status.CONFLICT);
    }
  }
}
