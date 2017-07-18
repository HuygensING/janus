package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/*
 * Contents of a document with a list of annotations.
 */
public class DocAndAnnotations {
  @JsonProperty
  public String text = null;

  @JsonProperty
  public List<Annotation> annotations = null;

  DocAndAnnotations(String text, List<Annotation> annotations) {
    this.text = text;
    this.annotations = annotations;
  }
}
