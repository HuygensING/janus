package nl.knaw.huygens.pergamon.janus.xml;

import nl.knaw.huygens.pergamon.janus.Annotation;
import nu.xom.Element;

public class TestTaggedBytes extends TestTaggedText {
  @Override
  protected TaggedText construct(Element doc) {
    return new TaggedBytes(doc);
  }

  @Override
  protected Annotation tag(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP) {
    return new Annotation(start8, end8, "", tag, null, "xml", tag);
  }
}
