package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;

public class TestAnnotatedCodepoints extends TestAnnotatedText {
  @Override
  protected AnnotatedText parse(Document doc) {
    return new AnnotatedCodepoints(doc);
  }

  @Override
  protected Annotation ann(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP) {
    return new Annotation(tag, startCP, endCP);
  }
}
