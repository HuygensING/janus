package nl.knaw.huygens.pergamon.janus.xml;

import nl.knaw.huygens.pergamon.janus.Annotation;
import nu.xom.Element;

public class TestTaggedUtf16 extends TestTaggedText {
  @Override
  protected TaggedText construct(Element doc) {
    return new TaggedUtf16(doc);
  }

  @Override
  protected Annotation tag(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP) {
    return new Annotation(start16, end16, "", tag, null, "xml", tag);
  }
}
