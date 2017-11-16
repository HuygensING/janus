package nl.knaw.huygens.pergamon.janus.xml;

import nl.knaw.huygens.pergamon.janus.Annotation;
import nu.xom.Element;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public abstract class TestTaggedText {
  private void test(String xml, String text, Annotation... reference) {
    TaggedText result = parse(xml);

    assertEquals(text, result.text());

    List<Annotation> tags = result.tags();
    assertEquals(reference.length, tags.size());

    for (int i = 0; i < reference.length; i++) {
      Annotation x = reference[i];
      Annotation y = tags.get(i);

      assertEquals(x.type, y.type);
      assertEquals(x.start, y.start);
      assertEquals(x.end, y.end);

      assertEquals(x.attributes, y.attributes);
    }
  }

  TaggedText parse(String xml) {
    try {
      return construct(XmlParser.fromString(xml).getRootElement());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract TaggedText construct(Element doc);

  // Constructs a Tag annotation. Implementations choose whether to use 8-bit, 16-bit, or codepoint end and start.
  // Tags may be reused as identifiers.
  protected abstract Annotation tag(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP);

  private Annotation tag(String tag,
                         int start8, int end8, int start16, int end16, int startCP, int endCP,
                         String... attrs) {
    Annotation ann = tag(tag, start8, end8, start16, end16, startCP, endCP);
    for (int i = 0; i < attrs.length; i += 2) {
      ann.attributes.put(attrs[i], attrs[i + 1]);
    }
    return ann;
  }

  @Test
  public void ascii() {
    test("<p>Hello, <b>world</b>!</p>", "Hello, world!",
      tag("p", 0, 13, 0, 13, 0, 13),
      tag("b", 7, 12, 7, 12, 7, 12));
  }

  @Test
  public void unicodeBmp() {
    test("<x>één</x>", "één", tag("x", 0, 5, 0, 3, 0, 3));
  }

  @Test
  public void astralPlane() {
    test("<tricky>𝄠<a/></tricky>", "𝄠",
      tag("tricky", 0, 4, 0, 2, 0, 1),
      tag("a", 4, 4, 2, 2, 1, 1));
  }

  @Test
  public void comments() {
    test("<foo><!-- what's next? --><bar/></foo>", "",
      tag("foo", 0, 0, 0, 0, 0, 0),
      tag("bar", 0, 0, 0, 0, 0, 0));
    // TODO test recording of comments
  }

  @Test
  public void namespaces() {
    test("<foo:x xmlns:foo='http://foo' xml:id='id'>" +
        "<bar:y xmlns:bar='http://bar' xmlns:quux='http://quux' quux:attr='dada'/>" +
        "<baz foo:attr='value'/></foo:x>", "",
      tag("foo:x", 0, 0, 0, 0, 0, 0,
        "xmlns:foo", "http://foo",
        "xml:id", "id"),
      tag("bar:y", 0, 0, 0, 0, 0, 0,
        "xmlns:bar", "http://bar",
        "quux:attr", "dada"),
      tag("baz", 0, 0, 0, 0, 0, 0,
        "foo:attr", "value"));

    test("<x xmlns='http://example.com/bla'><y/><z xmlns='http://z'><z2/></z></x>", "",
      tag("x", 0, 0, 0, 0, 0, 0, "xmlns", "http://example.com/bla"),
      tag("y", 0, 0, 0, 0, 0, 0, "xmlns", "http://example.com/bla"),
      tag("z", 0, 0, 0, 0, 0, 0, "xmlns", "http://z"),
      tag("z2", 0, 0, 0, 0, 0, 0, "xmlns", "http://z"));
  }

  @Test
  public void tagWithAttributes() {
    test("<xml foo='bar'> <tag attr2='quux' attr1='baz'/> </xml>",
      "  ",
      tag("xml", 0, 2, 0, 2, 0, 2, "foo", "bar"),
      tag("tag", 1, 1, 1, 1, 1, 1, "attr1", "baz", "attr2", "quux"));
  }

  @Test
  public void orderOfNestedTags() {
    test("<a><b><c/> bla bla <d><e/></d> <f/></b></a>",
      " bla bla  ",
      tag("a", 0, 10, 0, 10, 0, 10),
      tag("b", 0, 10, 0, 10, 0, 10),
      tag("c", 0, 0, 0, 0, 0, 0),
      tag("d", 9, 9, 9, 9, 9, 9),
      tag("e", 9, 9, 9, 9, 9, 9),
      tag("f", 10, 10, 10, 10, 10, 10)
    );
  }
}
