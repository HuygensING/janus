package nl.knaw.huygens.pergamon.janus.xml;

import com.fasterxml.jackson.annotation.JsonProperty;
import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;

import static java.util.stream.IntStream.range;

/**
 * Base class for XML-to-text-with-tags converters.
 */
public abstract class TaggedText {
  public final String docId;
  private final HashMap<Node, String> nodeId = new HashMap<>();
  final List<Tag> tags;
  final StringBuilder sb;

  TaggedText(Document doc) {
    this(doc, UUID.randomUUID().toString());
  }

  TaggedText(Document doc, String docId) {
    this.docId = docId;
    tags = new ArrayList<>();
    sb = new StringBuilder();
    nodeId.put(doc, docId);
    traverse(doc.getRootElement());
  }

  TaggedText(String text, String docId, List<Tag> tags) {
    this.docId = docId;
    sb = new StringBuilder(text);
    this.tags = tags;
  }

  /**
   * The tags corresponding to tags etc. in the original XML.
   *
   * @return An immutable list of tags.
   */
  @JsonProperty
  public List<Tag> tags() {
    return Collections.unmodifiableList(tags);
  }

  @JsonProperty
  public String text() {
    return sb.toString();
  }

  protected abstract void append(String s);

  protected abstract int offset();

  // Pre-order traversal of t.
  private void traverse(Node node) {
    if (node instanceof Text) {
      append(node.getValue());
    } else if (node instanceof Element) {
      String id = UUID.randomUUID().toString();
      nodeId.put(node, id);
      int base = offset();
      int insert = tags.size();
      tags.add(null); // To be filled in after the recursion, when we know the end offset.

      range(0, node.getChildCount()).forEach(i -> traverse(node.getChild(i)));

      String parentId = nodeId.get(node.getParent());
      if (parentId == null) {
        throw new NullPointerException("null parent id");
      }
      Tag tag = new Tag(id, ((Element) node).getQualifiedName(), base, offset(), docId, parentId);
      tags.set(insert, tag);
      range(0, ((Element) node).getAttributeCount())
        .forEach(i -> {
          Attribute attr = ((Element) node).getAttribute(i);
          tag.attributes.put(attr.getQualifiedName(), attr.getValue());
        });
    }
  }
}
