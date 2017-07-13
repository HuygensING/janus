package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTaggedCodepoints extends TestTaggedText {
  @Override
  protected TaggedText construct(Document doc) {
    return new TaggedCodepoints(doc);
  }

  @Override
  protected Tag tag(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP) {
    return new Tag(tag, tag, startCP, endCP, "", null);
  }

  private static final String HEADER = "<?xml version=\"1.0\"?>\n";

  private void test(String expect) {
    test(expect, expect);
  }

  private void test(String input, String expect) {
    String got = ((TaggedCodepoints) parse(input)).reconstruct().toXML();
    assertEquals(HEADER + expect + "\n", got);
  }

  @Test
  public void reconstruct() {
    test("<x><p><q /></p></x>");
    test("<x><p /><q /></x>");
    test("<p> hello <q>xml <r /> and json</q> world</p>");
    test("<tricky>\uD834\uDD20<a />\uD834\uDD20 got through! </tricky>");

    // Whitespace will be normalized, attributes sorted.
    test("<p   x=\"who?\" b=\"bla\"  z=\"foo\"  a=\"what?\"/>",
      "<p a=\"what?\" b=\"bla\" x=\"who?\" z=\"foo\" />");

    // XML namespaces should come out right.
    test("<x><y xmlns=\"http://y\"><y2 /><z:z xmlns:z=\"http://z\" /></y></x>");
  }
}
