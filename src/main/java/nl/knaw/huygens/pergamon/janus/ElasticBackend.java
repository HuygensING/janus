package nl.knaw.huygens.pergamon.janus;

import com.google.common.collect.ImmutableMap;
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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
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
    GetResponse response = client.prepareGet(documentIndex, documentType, id).get();
    if (!response.isExists()) {
      response = client.prepareGet(ANNOTATION_INDEX, ANNOTATION_TYPE, id).get();
      if (!response.isExists()) {
        return null;
      }
    }

    return ImmutableMap.of(
      "text", response.getSourceAsMap().get("body"),
      "annotations", getAnnotations(id, null, recursive));
  }

  @Override
  public List<Object> getAnnotations(String id, @Nullable String q, boolean recursive) {
    BoolQueryBuilder query = boolQuery().filter(termQuery(recursive ? "root" : "target", id));
    if (q != null) {
      query.must(queryStringQuery(q));
    }

    SearchResponse response = client.prepareSearch(ANNOTATION_INDEX)
                                    .setTypes(ANNOTATION_TYPE)
                                    .setQuery(query)
                                    // TODO: should we scroll, or should the client scroll?
                                    .setSize(1000).get();

    return Arrays.stream(response.getHits().getHits()).map(hit -> {
      Map<String, Object> map = hit.getSourceAsMap();
      // Move bodies into separate documents?
      map.remove("body");

      // Remove attributes used for internal purposes.
      map.remove("order");
      map.remove("root");
      map.remove("target");

      // Make returned object smaller on the wire.
      if (((Map<?, ?>) map.getOrDefault("attrib", EMPTY_MAP)).isEmpty()) {
        map.remove("attrib");
      }

      return map;
    }).collect(Collectors.toList());
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
      root = (String) got.getField("root").getValue();
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
  }

  @Override
  public PutResponse putXml(String id, TaggedCodepoints document) throws IOException {
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
  }
}
