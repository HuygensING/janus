package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.TreeMap;

public class Annotation {
  @JsonProperty
  public final Map<String, String> attributes = new TreeMap<>();

  @JsonProperty
  public final int start;

  @JsonProperty
  public final int end;

  @JsonProperty
  public final String type;

  public Annotation(String tag, int start, int end) {
    this.type = "tag:" + tag;
    this.end = end;
    this.start = start;
  }
}
