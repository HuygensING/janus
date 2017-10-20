package nl.knaw.huygens.pergamon.janus.graphql;

import graphql.schema.DataFetchingEnvironment;
import nl.knaw.huygens.pergamon.janus.ElasticBackend;

import java.util.List;
import java.util.stream.Collectors;

// TODO: merge functionality into Annotation class one package up?
public class Annotation {
  private final nl.knaw.huygens.pergamon.janus.Annotation annotation;

  public Annotation(nl.knaw.huygens.pergamon.janus.Annotation annotation) {
    this.annotation = annotation;
  }

  public String getAttribute(String key) {
    return annotation.attributes.get(key);
  }

  public List<Attribute> getAttributes() {
    return annotation.attributes.entrySet().stream()
                                .map(Attribute::new)
                                .collect(Collectors.toList());
  }

  public Document getBody(DataFetchingEnvironment env) {
    ElasticBackend backend = env.getContext();
    return Document.from(backend, annotation.body);
  }

  public String getId() {
    return annotation.id;
  }

  public int getStart() {
    return annotation.start;
  }

  public int getEnd() {
    return annotation.end;
  }

  public String getType() {
    return annotation.type;
  }

  public String getSource() {
    return annotation.source;
  }
}
