package nl.knaw.huygens.pergamon.janus;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestIdentifier {
  private static String[] valid = new String[]{
    "yes", "yes-sir", "yes_sir", "3y6891ytg23h9827h9843", "aaaa____bbbb----",
    "some_rather_long_filename_with_the_extension.xml.gz",
    // "AAAA____BBBB" ?
  };

  private static String[] invalid = new String[]{
    ".", "..", " hello", "foo!", "../file.xml", "dirname/basename",
  };

  @Test
  public void smokeTest() {
    for (String id : valid) {
      assertTrue(Identifier.valid(id));
    }
    for (String id : invalid) {
      assertFalse(Identifier.valid(id));
    }
  }
}
