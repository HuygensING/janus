package nl.knaw.huygens.pergamon.janus.xml;

import com.fasterxml.jackson.annotation.JsonProperty;
import nl.knaw.huygens.pergamon.janus.Annotation;
import nu.xom.Attribute;
import nu.xom.Comment;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static java.util.stream.IntStream.range;

/**
 * Base class for XML-to-text-with-tags converters.
 */
public abstract class TaggedText {
  public final String docId;
  private final HashMap<Node, String> nodeId = new HashMap<>();
  private Map<Integer, List<Comment>> comments = new HashMap<>();
  final List<Annotation> tags;
  final StringBuilder sb;

  TaggedText(Element doc) {
    this(doc, null);
  }

  TaggedText(Element doc, String docId) {
    if (docId == null) {
      docId = UUID.randomUUID().toString();
    }
    this.docId = docId;
    tags = new ArrayList<>();
    sb = new StringBuilder();
    nodeId.put(doc, docId);
    traverse(doc);
  }

  /**
   * The tags corresponding to tags etc. in the original XML.
   *
   * @return An immutable list of tags.
   */
  @JsonProperty
  public List<Annotation> tags() {
    return Collections.unmodifiableList(tags);
  }

  @JsonProperty
  public String text() {
    return sb.toString();
  }

  protected abstract void append(String s);

  // Current byte/halfword/codepoint offset into the text of the input.
  protected abstract int offset();

  private void traverse(Node node) {
    if (node instanceof Text) {
      append(node.getValue());
    } else if (node instanceof Element) {
      String id = UUID.randomUUID().toString();
      int base = offset();
      int insert = tags.size();
      tags.add(null); // To be filled in after the recursion, when we know the end offset.

      range(0, node.getChildCount()).forEach(i -> traverse(node.getChild(i)));

      Element elem = (Element) node;
      Annotation tag = new Annotation(base, offset(), docId, elem.getQualifiedName(), null, "xml", id);
      tags.set(insert, tag);
      range(0, elem.getAttributeCount())
        .forEach(i -> {
          Attribute attr = elem.getAttribute(i);
          tag.attributes.put(attr.getQualifiedName(), attr.getValue());
        });

      String uri = elem.getNamespaceURI();
      if (!"".equals(uri)) {
        String prefix = elem.getNamespacePrefix();
        if (!"".equals(prefix)) {
          tag.attributes.put("xmlns:" + prefix, uri);
        } else {
          tag.attributes.put("xmlns", uri);
        }
      }
    } else if (node instanceof Comment) {
      comments.compute(offset(), (k, v) -> {
        if (v == null) {
          v = new ArrayList<>();
        }
        // XXX Detaching the comment node seems to break the rest of the traversal
        //node.detach();
        v.add((Comment) node);
        return v;
      });
    }
  }

  // Pre-order traversal of t.
  // private void traverse(Node node) {
  //   if (node instanceof Text) {
  //     append(node.getValue());
  //   } else if (node instanceof Element) {
  //     String id = UUID.randomUUID().toString();
  //     nodeId.put(node, id);
  //     int base = offset();
  //     int insert = tags.size();
  //     tags.add(null); // To be filled in after the recursion, when we know the end offset.
  //
  //     range(0, node.getChildCount()).forEach(i -> traverse(node.getChild(i)));
  //
  //     String parentId = nodeId.get(node.getParent());
  //     if (parentId == null) {
  //       throw new NullPointerException("null parent id");
  //     }
  //     Element elem = (Element) node;
  //     Tag tag = new Tag(id, elem.getQualifiedName(), base, offset(), docId, parentId);
  //     tags.set(insert, tag);
  //     range(0, elem.getAttributeCount())
  //       .forEach(i -> {
  //         Attribute attr = elem.getAttribute(i);
  //         tag.attributes.put(attr.getQualifiedName(), attr.getValue());
  //       });
  //
  //     String uri = elem.getNamespaceURI();
  //     if (!"".equals(uri)) {
  //       String prefix = elem.getNamespacePrefix();
  //       if (!"".equals(prefix)) {
  //         tag.attributes.put("xmlns:" + prefix, uri);
  //       } else {
  //         tag.attributes.put("xmlns", uri);
  //       }
  //     }
  //   } else if (node instanceof Comment) {
  //     comments.compute(offset(), (k, v) -> {
  //       if (v == null) {
  //         v = new ArrayList<>();
  //       }
  //       // XXX Detaching the comment node seems to break the rest of the traversal
  //       //node.detach();
  //       v.add((Comment) node);
  //       return v;
  //     });
  //   }
  // }
}
