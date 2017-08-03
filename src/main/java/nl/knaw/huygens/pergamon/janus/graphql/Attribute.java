package nl.knaw.huygens.pergamon.janus.graphql;

import java.util.Map;

public class Attribute {
  private final Map.Entry<String, String> entry;

  Attribute(Map.Entry<String, String> entry) {
    this.entry = entry;
  }

  public String getKey() {
    return entry.getKey();
  }

  public String getValue() {
    return entry.getValue();
  }
}
