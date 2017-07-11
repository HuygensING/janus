package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
import nu.xom.Document;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface Backend extends AutoCloseable {
  class PutResult {
    // Id of document or annotation that was created.
    @JsonProperty
    public final String id;

    // HTTP status.
    @JsonProperty
    public final int status;

    PutResult(String id, int status) {
      this.id = id;
      this.status = status;
    }

    PutResult(String id, Response.Status status) {
      this(id, status.getStatusCode());
    }

    public Response asResponse() {
      return Response.status(status).entity(this).build();
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

  PutResult putAnnotation(Annotation ann, String id) throws IOException;

  PutResult putTxt(@Nullable String id, String content) throws IOException;

  default PutResult putXml(String id, Document document) throws IOException {
    return putXml(id, new TaggedCodepoints(document));
  }

  PutResult putXml(String id, TaggedCodepoints document) throws IOException;
}
