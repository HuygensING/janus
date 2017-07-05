package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.EMPTY_MAP;
import static org.elasticsearch.action.DocWriteRequest.OpType.CREATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
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
  public Map<String, Object> getWithAnnotations(String id) throws IOException {
    GetResponse response = client.prepareGet(documentIndex, documentType, id).get();
    if (!response.isExists()) {
      return null;
    }
    String body = (String) response.getSourceAsMap().get("body");

    HashMap<String, Object> result = new HashMap<>();
    result.put("text", body);
    result.put("annotations", getAnnotations(id));

    return result;
  }

  private List<Object> getAnnotations(String id) {
    SearchResponse response = client.prepareSearch(ANNOTATION_INDEX)
                                    .setTypes(ANNOTATION_TYPE)
                                    .setQuery(boolQuery().filter(termQuery("root", id)))
                                    // TODO: should we scroll, or should the client scroll?
                                    .setSize(1000).get();
    List<Object> result = new ArrayList<>();
    response.getHits().forEach(hit -> {
      Map<String, Object> map = hit.getSourceAsMap();
      // Move bodies into separate documents?
      map.remove("body");

      // Remove attributes used for internal purposes.
      map.remove("order");
      map.remove("root");
      map.remove("target");

      if (((Map<?, ?>) map.getOrDefault("attrib", EMPTY_MAP)).isEmpty()) {
        map.remove("attrib");
      }

      result.add(map);
    });
    return result;
  }

  @Override
  public int putAnnotation(Annotation ann, String id, String target) throws IOException {
    // If there's a document with the id, we annotate that, else the annotation with the id.
    // XXX we need to be smarter, e.g., address the document by index/type/id.
    GetResponse got = client.prepareGet(documentIndex, documentType, target).get();
    String root;
    if (got.isExists()) {
      root = id;
    } else {
      got = client.prepareGet(ANNOTATION_INDEX, ANNOTATION_TYPE, target).get();
      if (!got.isExists()) {
        return 404;
      }
      root = (String) got.getField("root").getValue();
    }

    IndexResponse response =
      client.prepareIndex(ANNOTATION_INDEX, ANNOTATION_TYPE, id)
            .setOpType(CREATE)
            .setSource(jsonBuilder()
              .startObject()
              .field("start", ann.start)
              .field("end", ann.end)
              .field("attrib", ann.attributes)
              .field("body", ann.type)
              .field("target", target)
              .field("root", root)
              .endObject()
            ).get();
    return response.status().getStatus();
  }

  @Override
  public int putXml(String id, Document document) throws IOException {
    TaggedText annotated = new TaggedCodepoints(document);

    IndexResponse response = client.prepareIndex(documentIndex, documentType, id)
                                   .setOpType(CREATE)
                                   .setSource(jsonBuilder()
                                     .startObject()
                                     .field("body", annotated.text())
                                     .endObject()
                                   ).get();
    int status = response.status().getStatus();
    if (status < 200 || status >= 300) {
      return status;
    }

    List<Tag> tags = annotated.tags();
    for (int i = 0; i < tags.size(); i++) {
      Tag ann = tags.get(i);
      response = client.prepareIndex(ANNOTATION_INDEX, ANNOTATION_TYPE, String.format("%s_tag%d", id, i))
                       .setOpType(CREATE)
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
                       ).get();
      status = response.status().getStatus();
      if (status < 200 || status >= 300) {
        return status;
      }
    }

    return 200;
  }
}
