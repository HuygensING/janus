package nl.knaw.huygens.pergamon.janus;

import io.dropwizard.jackson.Jackson;
import nl.knaw.huygens.pergamon.janus.xml.XmlParser;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.ParsingException;
import nu.xom.XPathException;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;

public class TestMapping {
  @Test
  public void basic() throws IOException, ParsingException {
    Map wantedMapping = Jackson.newObjectMapper().readValue(
      ("{" +
        " `_all`: {`enabled`: true}," +
        " `dynamic`: `strict`," +
        " `properties`: {" +
        " `body`: {`type`: `text`}," +
        " `published`: {`type`: `date`}," +
        " `author`: {`type`: `keyword`}" +
        "}}").replace('`', '"'),
      Map.class
    );

    List<Mapping.Field> fields = asList(
      new Mapping.Field("body", "text", "/foo/text"),
      new Mapping.Field("published", "date", "//@datepub"),
      new Mapping.Field("author", "keyword", "//author")
    );
    Mapping mapping = new Mapping(fields, true);

    assertEquals(wantedMapping, mapping.asMap());

    Document doc = XmlParser.fromString(
      "<foo datepub=`today`><author>me</author><text><p id=`1`>bla</p> <div>bla</div></text></foo>"
        .replace('`', '"'));

    HashMap<String, Object> wantedDoc = new HashMap<>();
    //wantedDoc.put("body", "bla bla");
    wantedDoc.put("published", "today");
    wantedDoc.put("author", "me");
    assertEquals(wantedDoc, mapping.apply(doc).getRight());
  }

  @Test
  public void namespaces() throws IOException, ParsingException {
    Mapping mapping = new Mapping(
      asList(new Mapping.Field("body", "text", "/foo:doc/bar:body")),
      asList(
        new Mapping.Namespace("foo", "http://example.com/foo"),
        new Mapping.Namespace("bar", "http://example.com/bar")),
      true);

    Document doc = XmlParser.fromString(
      "<foo:doc xmlns:foo=`http://example.com/foo` xmlns:bar=`http://example.com/bar`><bar:body>ok</bar:body></foo:doc>"
        .replace('`', '"'));
    Triple<String, Element, Map<String, String>> r = mapping.apply(doc);
    assertEquals("ok", r.getMiddle().getValue());
  }

  @Test(expected = IllegalArgumentException.class)
  public void duplicateField() {
    new Mapping(asList(
      new Mapping.Field("name", "keyword", "/name"),
      new Mapping.Field("name", "text", "//name")
    ), false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void noFields() {
    new Mapping(emptyList(), false);
  }

  @Test(expected = IllegalArgumentException.class)
  public void noValueForField() throws IOException, ParsingException {
    Mapping mapping = new Mapping(singletonList(new Mapping.Field("foo", "text", "/foo")), true);
    mapping.apply(XmlParser.fromString("<bar/>"));
  }

  @Test(expected = XPathException.class)
  public void invalidXPath() {
    new Mapping(singletonList(new Mapping.Field("title", "text", "gobble^dygook!")), true);
  }

  @Test(expected = XPathException.class)
  public void notANode() {
    new Mapping(singletonList(new Mapping.Field("title", "text", "/doc/title/string(.)")), true);
  }
}
