package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;

/*
 * Contents of a document with a list of annotations.
 */
public class DocAndAnnotations {
  @JsonProperty
  public Map<String, Object> body;

  @JsonProperty
  public String id = null;

  @JsonProperty
  public String text = null;

  @JsonProperty
  public List<Annotation> annotations = null;

  DocAndAnnotations(String id, Map<String, Object> body, List<Annotation> annotations) {
    this.id = id;
    this.body = body;
    this.text = (String) body.get("body");
    this.annotations = annotations;
  }
}
