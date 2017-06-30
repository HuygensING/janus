package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;

public class TestAnnotatedUtf16 extends TestAnnotatedText {
  @Override
  protected AnnotatedText parse(Document doc) {
    return new AnnotatedUtf16(doc);
  }

  @Override
  protected Annotation ann(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP) {
    return new Annotation(tag, start16, end16);
  }
}
