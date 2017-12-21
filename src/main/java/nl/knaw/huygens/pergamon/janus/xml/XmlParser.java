package nl.knaw.huygens.pergamon.janus.xml;

import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.ParsingException;
import org.apache.commons.text.StringEscapeUtils;

import javax.xml.stream.XMLEventFactory;
import javax.xml.stream.XMLEventReader;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.events.DTD;
import javax.xml.stream.events.XMLEvent;
import javax.xml.stream.util.EventReaderDelegate;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import static javax.xml.stream.XMLStreamConstants.START_DOCUMENT;


// Wraps thread-local nu.xom.Builders.
// http://xom.nu/designprinciples.xhtml#d680e169
public class XmlParser {
  private XmlParser() {
  }

  private static final ThreadLocal<Builder> BUILDER =
    new ThreadLocal<Builder>() {
      @Override
      protected Builder initialValue() {
        return new Builder();
      }
    };

  public static Document fromString(String s) throws IOException, ParsingException {
    return BUILDER.get().build(new StringReader(s));
  }

  // Inserts a DTD into a document.
  // https://stackoverflow.com/questions/1096365/validate-an-xml-file-against-local-dtd-file-with-java
  private static class InjectDTD extends EventReaderDelegate {
    private boolean dtdNext;
    private final DTD dtd;

    InjectDTD(XMLEventReader r, DTD dtd) {
      super(r);
      this.dtd = dtd;
    }

    @Override
    public XMLEvent nextEvent() throws XMLStreamException {
      if (dtdNext) {
        dtdNext = false;
        return dtd;
      }
      XMLEvent e = super.nextEvent();
      if (e.getEventType() == START_DOCUMENT) {
        dtdNext = true;
      }
      return e;
    }
  }

  /**
   * Validates XML against a DTD and returns any errors found.
   *
   * @param content XML document.
   * @param dtd     Filename of DTD.
   * @return A list of errors.
   * @throws XMLStreamException
   */
  public static List<String> validate(String content, String dtd) throws XMLStreamException {
    XMLInputFactory f = XMLInputFactory.newInstance();

    List<String> errors = new ArrayList<>();

    f.setXMLReporter(((msg, type, info, loc) -> errors.add(String.format("%s: %s at %s", msg, type, loc))));

    // Strip existing DTDs from the XML,
    XMLEventReader r = f.createFilteredReader(
      f.createXMLEventReader(new StringReader(content)),
      e -> e.getEventType() != XMLStreamConstants.DTD);
    // then insert our own.
    r = new InjectDTD(r,
      XMLEventFactory.newInstance().createDTD(String.format(
        "<!DOCTYPE Employee SYSTEM \"%s\">",
        StringEscapeUtils.escapeJava(dtd))));

    while (r.hasNext()) {
      r.next();
    }
    return errors;
  }
}
