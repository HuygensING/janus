package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Document;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class TestTaggedText {
  private void test(String xml, String text, Tag... reference) {
    TaggedText result = parse(xml);

    assertEquals(text, result.text());

    List<Tag> tags = result.tags();
    assertEquals(reference.length, tags.size());

    for (int i = 0; i < reference.length; i++) {
      Tag x = reference[i];
      Tag y = tags.get(i);

      assertEquals(x.type, y.type);
      assertEquals(x.start, y.start);
      assertEquals(x.end, y.end);

      Iterator<Map.Entry<String, String>> iterx = x.attributes.entrySet().iterator();
      Iterator<Map.Entry<String, String>> itery = y.attributes.entrySet().iterator();

      while (iterx.hasNext()) {
        Map.Entry<String, String> attrx = iterx.next();
        Map.Entry<String, String> attry = itery.next();
        assertEquals(attrx.getKey(), attry.getKey());
        assertEquals(attrx.getValue(), attry.getValue());
      }
    }
  }

  TaggedText parse(String xml) {
    try {
      return construct(XmlParser.fromString(xml));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  protected abstract TaggedText construct(Document doc);

  // Constructs a Tag annotation. Implementations choose whether to use 8-bit, 16-bit, or codepoint end and start.
  // Tags may be reused as identifiers.
  protected abstract Tag tag(String tag, int start8, int end8, int start16, int end16, int startCP, int endCP);

  private Tag tag(String tag,
                  int start8, int end8, int start16, int end16, int startCP, int endCP,
                  String... attrs) {
    Tag ann = tag(tag, start8, end8, start16, end16, startCP, endCP);
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
