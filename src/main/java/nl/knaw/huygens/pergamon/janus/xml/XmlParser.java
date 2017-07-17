package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.ParsingException;

import java.io.IOException;
import java.io.StringReader;

// Wraps a global nu.xom.Builder that we use in various places.
public class XmlParser {
  private XmlParser() {
  }

  private static final Builder BUILDER = new Builder();

  public static Document fromString(String s) throws IOException, ParsingException {
    return BUILDER.build(new StringReader(s));
  }
}
