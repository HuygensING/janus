package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import nu.xom.Attribute;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Node;
import nu.xom.Text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.stream.IntStream.range;

/**
 * Base class for XML-to-text-with-annotations converters.
 */
abstract class AnnotatedText {
  private final List<Annotation> annotations = new ArrayList<>();
  final StringBuilder sb = new StringBuilder();

  AnnotatedText(Document doc) {
    traverse(doc.getRootElement());
  }

  /**
   * The annotations corresponding to tags etc. in the original XML.
   *
   * @return An immutable list of annotations.
   */
  @JsonProperty
  public List<Annotation> annotations() {
    return Collections.unmodifiableList(annotations);
  }

  @JsonProperty
  public String text() {
    return sb.toString();
  }

  protected abstract void append(String s);

  protected abstract int offset();

  // Pre-order traversal of t.
  void traverse(Node node) {
    if (node instanceof Text) {
      append(node.getValue());
    } else if (node instanceof Element) {
      int base = offset();
      int insert = annotations.size();
      annotations.add(null);

      range(0, node.getChildCount()).forEach(i -> traverse(node.getChild(i)));

      Annotation ann = new Annotation(((Element) node).getQualifiedName(), base, offset());
      annotations.set(insert, ann);
      range(0, ((Element) node).getAttributeCount())
        .forEach(i -> {
          Attribute attr = ((Element) node).getAttribute(i);
          ann.attributes.put(attr.getQualifiedName(), attr.getValue());
        });
    }
  }
}
