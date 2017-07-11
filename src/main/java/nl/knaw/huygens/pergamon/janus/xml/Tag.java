package nl.knaw.huygens.pergamon.janus.xml;

import com.fasterxml.jackson.annotation.JsonProperty;
import nl.knaw.huygens.pergamon.janus.Annotation;

/**
 * A Tag represents an XML element, one of the types of annotations that we support.
 */
public class Tag extends Annotation {
  @JsonProperty
  public final String id;

  public Tag(String id, String tag, int start, int end, String target) {
    super(start, end, target, tag, "", "tag");
    this.id = id;
  }
}
