package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Document;
import nu.xom.Element;

import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;

/**
 * Converts XML to text-with-annotations. Uses UTF-16 offsets.
 */
public class TaggedCodepoints extends TaggedText {
  private int offset = 0;

  public TaggedCodepoints(Document doc) {
    super(doc);
  }

  public TaggedCodepoints(String text, List<Tag> tags) {
    super(text, tags);
    int length = text.codePointCount(0, sb.length());
    // TODO check if tags are sorted
    for (Tag tag : tags) {
      if (tag.start > tag.end) {
        throw new IllegalArgumentException("tag start must be < tag end");
      } else if (tag.start < 0 || tag.end > length) {
        throw new IllegalArgumentException("tag out of bounds");
      }
    }
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

  /**
   * Reconstruct XML document from text and list of tags.
   */
  public Document reconstruct() {
    return new Document(reconstruct(tags.get(0), 0, tags.stream().collect(groupingBy(t -> t.target))));
  }

  private Element reconstruct(Tag root, int textIndex, Map<String, List<Tag>> childrenOf) {
    // Children are sorted by start.
    Element elem = new Element(root.tag);

    for (Tag child : childrenOf.getOrDefault(root.id, emptyList())) {
      if (child.start > textIndex) {
        // TODO use codepoints
        elem.appendChild(sb.substring(textIndex, child.start));
        textIndex = child.start;
      }

      elem.appendChild(reconstruct(child, textIndex, childrenOf));
    }

    return elem;
  }
}
