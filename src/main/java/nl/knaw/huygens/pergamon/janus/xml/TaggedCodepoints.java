package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Attribute;
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

  public TaggedCodepoints(Document doc, String id) {
    super(doc, id);
  }

  public TaggedCodepoints(String text, String docId, List<Tag> tags) {
    super(text, docId, tags);
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
    Element root = new Reconstruction(tags.stream().collect(groupingBy(t -> t.xmlParent))).apply(tags.get(0));
    return new Document(root);
  }

  private class Reconstruction {
    private Map<String, List<Tag>> children;
    private int cpIndex = 0; // code point index
    private int sbIndex = 0; // index into StringBuilder

    Reconstruction(Map<String, List<Tag>> children) {
      this.children = children;
    }

    private Element apply(Tag root) {
      // Children are sorted by start.
      Element elem = new Element(root.tag);
      root.attributes.forEach((k, v) -> elem.addAttribute(new Attribute(k, v)));

      for (Tag child : children.getOrDefault(root.id, emptyList())) {
        if (child.start > sbIndex) {
          int end = sb.offsetByCodePoints(sbIndex, child.start - cpIndex);
          elem.appendChild(sb.substring(sbIndex, end));
          cpIndex = child.start;
          sbIndex = end;
        }

        elem.appendChild(apply(child));
      }
      if (sbIndex < root.end) {
        int end = sb.offsetByCodePoints(sbIndex, root.end - cpIndex);
        elem.appendChild(sb.substring(sbIndex, end));
        cpIndex = root.end;
        sbIndex = end;
      }

      return elem;
    }
  }
}
