package nl.knaw.huygens.pergamon.janus;

import nu.xom.Document;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public abstract class TestAnnotatedText {
  private void test(String xml, String text, Annotation... reference) {
    AnnotatedText result;
    try {
      result = parse(new nu.xom.Builder().build(new ByteArrayInputStream(xml.getBytes())));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    assertEquals(text, result.text());

    List<Annotation> annotations = result.annotations();
    assertEquals(reference.length, annotations.size());

    for (int i = 0; i < reference.length; i++) {
      Annotation x = reference[i];
      Annotation y = annotations.get(i);

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

  protected abstract AnnotatedText parse(Document doc);

  // Constructs ann annotation with the required tag, start, end.
  // Implementations choose whether to use 8-bit, 16-bit, or codepoint end and start.
  protected abstract Annotation ann(String tag,
                                    int start8, int end8, int start16, int end16, int startCP, int endCP);

  private Annotation ann(String tag,
                         int start8, int end8, int start16, int end16, int startCP, int endCP,
                         String... attrs) {
    Annotation ann = ann(tag, start8, end8, start16, end16, startCP, endCP);
    for (int i = 0; i < attrs.length; i += 2) {
      ann.attributes.put(attrs[i], attrs[i + 1]);
    }
    return ann;
  }

  @Test
  public void ascii() {
    test("<p>Hello, <b>world</b>!</p>", "Hello, world!",
      ann("p", 0, 13, 0, 13, 0, 13),
      ann("b", 7, 12, 7, 12, 7, 12));
  }

  @Test
  public void unicodeBmp() {
    test("<x>één</x>", "één", ann("x", 0, 5, 0, 3, 0, 3));
  }

  @Test
  public void astralPlane() {
    test("<tricky>𝄠<a/></tricky>", "𝄠",
      ann("tricky", 0, 4, 0, 2, 0, 1),
      ann("a", 4, 4, 2, 2, 1, 1));
  }

  @Test
  public void tagWithAttributes() {
    test("<xml foo='bar'> <tag attr2='quux' attr1='baz'/> </xml>",
      "  ",
      ann("xml", 0, 2, 0, 2, 0, 2, "foo", "bar"),
      ann("tag", 1, 1, 1, 1, 1, 1, "attr1", "baz", "attr2", "quux"));
  }

  @Test
  public void orderOfNestedTags() {
    test("<a><b><c/> bla bla <d><e/></d> <f/></b></a>",
      " bla bla  ",
      ann("a", 0, 10, 0, 10, 0, 10),
      ann("b", 0, 10, 0, 10, 0, 10),
      ann("c", 0, 0, 0, 0, 0, 0),
      ann("d", 9, 9, 9, 9, 9, 9),
      ann("e", 9, 9, 9, 9, 9, 9),
      ann("f", 10, 10, 10, 10, 10, 10)
    );
  }
}
