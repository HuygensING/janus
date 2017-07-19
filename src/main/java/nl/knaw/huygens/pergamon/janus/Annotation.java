package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.TreeMap;

/**
 * An annotation on a document.
 * <p>
 * The document being annotated is called the target and referenced by its id.
 * Optionally, an annotation may have a body, which is in turn a document,
 * so that annotations can form links between documents.
 */
public class Annotation {
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public final Map<String, String> attributes = new TreeMap<>();

  // id of this annotation. Must be null on input.
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String id;

  @JsonProperty
  public final int start;

  @JsonProperty
  public final int end;

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public final String body;

  @JsonProperty
  public final String tag;

  /**
   * id of annotated document or annotation.
   */
  @JsonProperty
  public String target;

  @JsonProperty
  public final String type;

  public Annotation() {
    this(0, 0, "", "", "", "", null);
  }

  public Annotation(int start, int end, String target, String tag, String body, String type, String id) {
    this.start = start;
    this.end = end;
    this.target = target;
    this.body = body;
    this.tag = tag;
    this.type = type;
    this.id = id;
  }
}
