package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.TreeMap;

/**
 * A Tag represents an XML tag, one of the types of annotations that we support.
 */
public class Tag {
  @JsonProperty
  public final Map<String, String> attributes = new TreeMap<>();

  @JsonProperty
  public final int start;

  @JsonProperty
  public final int end;

  @JsonProperty
  public final String tag;

  public Tag() {
    this("", 0, 0);
  }

  public Tag(String tag, int start, int end) {
    this.tag = tag;
    this.end = end;
    this.start = start;
  }
}
