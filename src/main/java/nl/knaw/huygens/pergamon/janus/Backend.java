package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
import nl.knaw.huygens.pergamon.janus.xml.XmlParser;
import nu.xom.ParsingException;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

public interface Backend extends AutoCloseable {
  // Part of a paginated list.
  class ListPage {
    @JsonProperty
    public List<String> result;

    @JsonProperty
    public int from;

    @JsonProperty
    public long total;

    ListPage(int from, long total, List<String> result) {
      this.result = result;
      this.from = from;
      this.total = total;
    }
  }

  // To be returned by PUT/POST methods.
  class PutResult {
    // Id of document or annotation that was created.
    @JsonProperty
    public final String id;

    // HTTP status.
    @JsonProperty
    public final int status;

    @JsonInclude(value = JsonInclude.Include.NON_EMPTY)
    public final String message;

    PutResult(String id, int status, String message) {
      this.id = id;
      this.status = status;
      this.message = message;
    }

    PutResult(String id, int status) {
      this(id, status, null);
    }

    PutResult(String id, Response.Status status, String message) {
      this(id, status.getStatusCode(), message);
    }

    PutResult(String id, Response.Status status) {
      this(id, status, null);
    }

    Response asResponse() {
      return Response.status(status).entity(this).build();
    }
  }

  /**
   * Returns a Response with result as the entity. A null result becomes a 404, non-null a 200.
   * Intended to wrap the result of GETs.
   */
  static Response asResponse(Object result) {
    return (result == null ? Response.status(NOT_FOUND) : Response.status(OK).entity(result)).build();
  }

  /**
   * Get the single annotation with the given id.
   */
  Annotation getAnnotation(String id);

  /**
   * Get annotations belong to id, optionally satisfying the query string q.
   * <p>
   * If recursive, get annotations on annotations as well.
   */
  List<Annotation> getAnnotations(String id, @Nullable String q, boolean recursive);

  /**
   * Retrieve the document with the given id and its annotations.
   * <p>
   * Returns null if no document has the given id.
   * <p>
   * If recursive, get annotations on annotations as well.
   */
  DocAndAnnotations getWithAnnotations(String id, boolean recursive) throws IOException;

  /**
   * Produce reconstruction of XML document.
   */
  String getXml(String id) throws IOException;

  default PutResult putAnnotation(@Nullable String target, Annotation ann) throws IOException {
    if (ann.id != null) {
      return new PutResult(ann.id, BAD_REQUEST, "annotation may not determine its own id");
    }
    if (target != null && !Objects.equals(target, ann.target)) {
      return new PutResult(null, BAD_REQUEST,
        String.format("target mismatch: '%s' in path, '%s' in annotation", target, ann.target));
    }
    ann.target = target;
    return putAnnotation(ann);
  }

  /**
   * List documents ids in index, with optional full-text search.
   *
   * @param query Query string (Lucene syntax). null to get all documents.
   * @return List of matching document ids.
   */
  ListPage listDocs(String query, int from, int count);

  /**
   * Stores the annotation ann, which must have its target set.
   */
  PutResult putAnnotation(Annotation ann) throws IOException;

  PutResult putTxt(@Nullable String id, String content) throws IOException;

  default PutResult putXml(String id, String document) throws IOException {
    try {
      return putXml(new TaggedCodepoints(XmlParser.fromString(document), id));
    } catch (ParsingException e) {
      return new PutResult(id, BAD_REQUEST, e.toString());
    }
  }

  PutResult putXml(TaggedCodepoints document) throws IOException;
}
