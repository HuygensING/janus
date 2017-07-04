package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;

public class TestTaggedBytes extends TestTaggedText {
  @Override
  protected TaggedText parse(Document doc) {
    return new TaggedBytes(doc);
  }

  @Override
  protected Tag tag(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP) {
    return new Tag(tag, start8, end8);
  }
}
