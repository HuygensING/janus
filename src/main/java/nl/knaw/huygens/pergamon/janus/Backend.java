package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;

import java.io.IOException;
import java.util.Map;

public interface Backend extends AutoCloseable {
  /**
   * Returns an HTTP status code.
   */
  int putAnnotation(Annotation ann, String id, String target) throws IOException;

  /**
   * Returns null if no document has the given id.
   */
  Map<String, Object> getWithAnnotations(String id) throws IOException;

  /**
   * Returns an HTTP status code.
   */
  int putXml(String id, Document document) throws IOException;
}
