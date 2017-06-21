package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;

public class TestAnnotatedCodepoints extends TestAnnotatedText {
  @Override
  protected AnnotatedText parse(Document doc) {
    return new AnnotatedCodepoints(doc);
  }

  @Override
  protected Annotation ann(String tag, int offset8, int length8, int offset16, int length16, int offsetCP,
                           int lengthCP) {
    return new Annotation(tag, offsetCP, lengthCP);
  }
}
