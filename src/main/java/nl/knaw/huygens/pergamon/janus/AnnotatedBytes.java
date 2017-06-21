package nl.knaw.huygens.pergamon.janus;

import com.google.common.base.Utf8;
import nu.xom.Document;

/**
 * Converts XML to text-with-annotations. Uses byte offsets.
 */
public class AnnotatedBytes extends AnnotatedText {
  private int offset = 0;

  public AnnotatedBytes(Document doc) {
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
