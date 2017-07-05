package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Backend extends AutoCloseable {
  class PutResponse {
    // Id of document or annotation that was created.
    public final String id;

    // HTTP status.
    public final int status;

    PutResponse(String id, int status) {
      this.id = id;
      this.status = status;
    }
  }

  /**
   * Get annotations belong to id, optionally satisfying the query string q.
   */
  List<Object> getAnnotations(String id, @Nullable String q);

  PutResponse putAnnotation(Annotation ann, String id, String target) throws IOException;

  /**
   * Returns null if no document has the given id.
   */
  Map<String, Object> getWithAnnotations(String id) throws IOException;

  PutResponse putXml(String id, Document document) throws IOException;
}
