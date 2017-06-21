package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.TreeMap;

public class Annotation {
  @JsonProperty
  public final Map<String, String> attributes = new TreeMap<>();

  @JsonProperty
  public final int offset;

  @JsonProperty
  public final int length;

  @JsonProperty
  public final String tag;

  public Annotation(String tag, int offset, int length) {
    this.tag = tag;
    this.length = length;
    this.offset = offset;
  }
}
