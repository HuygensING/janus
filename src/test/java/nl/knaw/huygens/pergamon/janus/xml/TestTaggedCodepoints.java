package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Document;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestTaggedCodepoints extends TestTaggedText {
  @Override
  protected TaggedText parse(Document doc) {
    return new TaggedCodepoints(doc);
  }

  @Override
  protected Tag tag(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP) {
    return new Tag(tag, tag, startCP, endCP, "");
  }

  @Test
  public void reconstruct() {
    String xml = "<?xml version=\"1.0\"?>\n<x><p><q /></p></x>\n";
    assertEquals(xml, ((TaggedCodepoints) parse(xml)).reconstruct().toXML());

    xml = "<?xml version=\"1.0\"?>\n<x><p /><q /></x>\n";
    assertEquals(xml, ((TaggedCodepoints) parse(xml)).reconstruct().toXML());
  }
}
