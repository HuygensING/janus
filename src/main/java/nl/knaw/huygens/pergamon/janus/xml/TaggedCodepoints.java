package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Element;

/**
 * Converts XML to text-with-annotations. Uses UTF-16 offsets.
 */
public class TaggedCodepoints extends TaggedText {
  private int offset = 0;

  public TaggedCodepoints(Element doc) {
    super(doc);
  }

  public TaggedCodepoints(Element doc, String id) {
    super(doc, id);
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
