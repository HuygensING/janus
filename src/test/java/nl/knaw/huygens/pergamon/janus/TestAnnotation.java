package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;

public class TestAnnotation {
  @Test
  public void emptyBody() throws IOException {
    ObjectMapper mapper = new ObjectMapper();
    Annotation ann = mapper.readValue("{\"start\": 1, \"end\": 2}", Annotation.class);
    String s = mapper.writeValueAsString(ann);
    assertFalse(s.contains("body"));
  }
}
