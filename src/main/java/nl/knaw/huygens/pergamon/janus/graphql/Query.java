package nl.knaw.huygens.pergamon.janus.graphql;

import com.coxautodev.graphql.tools.GraphQLQueryResolver;
import nl.knaw.huygens.pergamon.janus.Backend;

import java.util.List;
import java.util.stream.Collectors;

public class Query implements GraphQLQueryResolver {
  private final Backend backend;

  public Query(Backend backend) {
    this.backend = backend;
  }

  public Annotation annotation(String id) {
    nl.knaw.huygens.pergamon.janus.Annotation ann = backend.getAnnotation(id);
    return ann == null ? null : new Annotation(ann);
  }

  public Document document(String id) {
    return Document.from(backend, id);
  }

  public List<Document> fulltext(String query, int from, int count) {
    return backend.listDocs(query, from, count).result
      .stream()
      .map(id -> Document.from(backend, id))
      .collect(Collectors.toList());
  }
}
