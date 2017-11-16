package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Element;

public class TestTaggedCodepoints extends TestTaggedText {
  @Override
  protected TaggedText construct(Element doc) {
    return new TaggedCodepoints(doc);
  }

  @Override
  protected Tag tag(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP) {
    return new Tag(tag, tag, startCP, endCP, "", null);
  }
}
