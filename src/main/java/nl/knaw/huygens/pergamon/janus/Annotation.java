package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.TreeMap;

// A full-blown annotation.
public class Annotation {
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public final Map<String, String> attributes = new TreeMap<>();

  @JsonProperty
  public final int start;

  @JsonProperty
  public final int end;

  @JsonProperty
  public final String body;

  @JsonProperty
  public final String tag;

  @JsonProperty
  public final String type;

  @JsonProperty
  public final String target;

  public Annotation() {
    this(0, 0, "", "", "", "");
  }

  public Annotation(int start, int end, String body, String tag, String type, String target) {
    this.start = start;
    this.end = end;
    this.body = body;
    this.tag = tag;
    this.type = type;
    this.target = target;
  }
}
