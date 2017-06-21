package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;

/**
 * Converts XML to text-with-annotations. Uses UTF-16 offsets.
 */
public class AnnotatedCodepoints extends AnnotatedText {
  private int offset = 0;

  public AnnotatedCodepoints(Document doc) {
    super(doc);
  }

  @Override
  protected void append(String s) {
    sb.append(s);
    offset += s.codePointCount(0, s.length());
  }

  @Override
  protected int offset() {
    return offset;
  }
}
