package nl.knaw.huygens.pergamon.janus.xml;

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
  public final String id;

  @JsonProperty
  public final int start;

  @JsonProperty
  public final int end;

  @JsonProperty
  public final String tag;

  @JsonProperty
  public final String target;

  public Tag(String id, String tag, int start, int end, String target) {
    this.id = id;
    this.tag = tag;
    this.end = end;
    this.start = start;
    this.target = target;
  }
}
