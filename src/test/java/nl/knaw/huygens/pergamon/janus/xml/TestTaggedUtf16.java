package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Document;

public class TestTaggedUtf16 extends TestTaggedText {
  @Override
  protected TaggedText construct(Document doc) {
    return new TaggedUtf16(doc);
  }

  @Override
  protected Tag tag(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP) {
    return new Tag(tag, tag, start16, end16, "", "");
  }
}
