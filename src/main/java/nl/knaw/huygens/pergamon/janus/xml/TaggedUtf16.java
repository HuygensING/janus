package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Element;

/**
 * Converts XML to text-with-annotations. Uses UTF-16 offsets.
 * <p>
 * UTF-16 offsets are useful for Java and JavaScript usage: those languages allow fast indexing/slicing using these
 * offsets.
 */
public class TaggedUtf16 extends TaggedText {
  public TaggedUtf16(Element doc) {
    super(doc);
  }

  @Override
  protected void append(String s) {
    sb.append(s);
  }

  @Override
  protected int offset() {
    return sb.length();
  }
}
