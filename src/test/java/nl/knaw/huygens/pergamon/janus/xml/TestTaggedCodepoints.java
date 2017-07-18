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
    String got = ((TaggedCodepoints) parse(expect)).reconstruct().toXML();
    assertEquals(HEADER + expect + "\n", got);
  }

  @Test
  public void reconstruct() {
    test("<x><p><q /></p></x>");
    test("<x><p /><q /></x>");
    test("<p> hello <q>xml <r /> and json</q> world</p>");
    test("<p at=\"what?\" />");
  }
}
