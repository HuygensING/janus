package nl.knaw.huygens.pergamon.janus.graphql;

import graphql.schema.DataFetchingEnvironment;
import nl.knaw.huygens.pergamon.janus.DocAndAnnotations;
import nl.knaw.huygens.pergamon.janus.ElasticBackend;

import java.util.List;
import java.util.stream.Collectors;

// TODO: merge with DocAndAnnotations?
public class Document {
  private final String id;
  private final String text;

  public static Document from(ElasticBackend backend, String id) {
    // TODO need Backend::getDocument
    DocAndAnnotations doc = backend.getWithAnnotations(id, false);
    return doc == null ? null : new Document(doc);
  }

  private Document(DocAndAnnotations doc) {
    this.id = doc.id;
    this.text = doc.text;
  }

  public List<Annotation> getAnnotations(DataFetchingEnvironment env) {
    ElasticBackend backend = env.getContext();
    return backend.getAnnotations(id, null, false)
                  .stream()
                  .map(Annotation::new)
                  .collect(Collectors.toList());
  }

  public String getId() {
    return id;
  }

  public String getText() {
    return text;
  }
}
