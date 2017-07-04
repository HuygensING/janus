package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;

import java.io.IOException;
import java.util.Map;

public interface Backend extends AutoCloseable {
  int putAnnotation(Annotation ann, String id, String target) throws IOException;

  Map<String, Object> getWithAnnotations(String id) throws IOException;

  int putXml(String id, Document document) throws IOException;
}
