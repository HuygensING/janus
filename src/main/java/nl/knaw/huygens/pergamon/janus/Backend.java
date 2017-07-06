package nl.knaw.huygens.pergamon.janus;

import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
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
   * <p>
   * If recursive, get annotations on annotations as well.
   */
  List<Object> getAnnotations(String id, @Nullable String q, boolean recursive);

  /**
   * Retrieve the document with the given id and its annotations.
   * <p>
   * Returns null if no document has the given id.
   * <p>
   * If recursive, get annotations on annotations as well.
   */
  Map<String, Object> getWithAnnotations(String id, boolean recursive) throws IOException;

  PutResponse putAnnotation(Annotation ann, String id, String target) throws IOException;

  PutResponse putTxt(@Nullable String id, String content) throws IOException;

  default PutResponse putXml(String id, Document document) throws IOException {
    return putXml(id, new TaggedCodepoints(document));
  }

  PutResponse putXml(String id, TaggedCodepoints document) throws IOException;
}
