package nl.knaw.huygens.pergamon.janus.xml;

import com.google.common.base.Utf8;
import nu.xom.Element;

/**
 * Converts XML to text-with-annotations. Uses byte offsets.
 */
public class TaggedBytes extends TaggedText {
  private int offset = 0;

  public TaggedBytes(Element doc) {
    super(doc);
  }

  @Override
  protected void append(String s) {
    sb.append(s);
    offset += Utf8.encodedLength(s);
  }

  @Override
  protected int offset() {
    return offset;
  }
}
