package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import nl.knaw.huygens.pergamon.janus.xml.XmlParser;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Nodes;
import nu.xom.ParsingException;
import org.apache.commons.lang3.tuple.Triple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Mapping represents an Elasticsearch mapping (schema).
 */
public class Mapping {
  private static final Logger logger = LoggerFactory.getLogger(Mapping.class);

  /**
   * An Elasticsearch field, with the XPath expression used to populate it.
   */
  public static class Field {
    // Name of field in Elasticsearch
    @JsonProperty
    private String name;
    // Type of field, e.g., keyword, date.
    @JsonProperty
    private String type;
    // XPath expression to find field value in XML.
    @JsonProperty
    private String xpath;

    Field() {
    }

    Field(String name, String type, String xpath) {
      this.name = name;
      this.type = type;
      this.xpath = xpath;
    }
  }

  private final boolean strict;
  private final List<Field> fields;

  // Elasticsearch mapping, in a json'able format.
  private final Map<String, Object> mapping;

  // Static parts of mapping.
  private static Map<String, Boolean> _ALL = new HashMap<>();

  private static final Document NULLDOC;

  static {
    _ALL.put("enabled", true);

    try {
      NULLDOC = XmlParser.fromString("<x/>");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Construct mapping from a list of Fields.
   *
   * @param fields List of fields. The first field is special, in that offsets are calculated relative to this field's
   *               text.
   * @param strict Whether to throw an exception when an XPath expression returns no result.
   */
  public Mapping(List<Field> fields, boolean strict) {
    if (fields.isEmpty()) {
      throw new IllegalArgumentException("mapping must have at least one field");
    }
    this.fields = new ArrayList<>(fields); // defensive copy.
    this.strict = strict;

    mapping = new HashMap<>();
    mapping.put("_all", _ALL);
    mapping.put("dynamic", "strict");

    HashMap<String, Object> properties = new HashMap<>();
    mapping.put("properties", properties);

    fields.forEach(field -> {
      if (properties.containsKey(field.name)) {
        throw new IllegalArgumentException("duplicate field name " + field.name);
      }

      // Validate XPath by trying it on a trivial document.
      NULLDOC.query(field.xpath);

      Map<String, String> typeMap = new HashMap<>();
      typeMap.put("type", field.type);
      properties.put(field.name, typeMap);
    });
  }

  /*
   * Apply mapping to document to get field values.
   *
   * Returns:
   * - the name of the first ("body") field,
   * - an Element that contains the main body field for tag/annotation extraction,
   * - a Map that can be serialized to JSON and fed to Elasticsearch.
   */
  public Triple<String, Element, Map<String, String>> apply(Document doc) throws IOException, ParsingException {
    Element root = doc.getRootElement();

    Element body;
    try {
      body = (Element) root.query(fields.get(0).xpath).get(0);
    } catch (IndexOutOfBoundsException e) {
      throw new IllegalArgumentException("no value for body field '" + fields.get(0).name + "'");
    }
    Map<String, String> fieldValues = new HashMap<>();

    for (Field field : fields.subList(1, fields.size())) {
      Nodes values = root.query(field.xpath);
      if (values.size() < 1) {
        String msg = "no value for field " + field.name;
        if (strict) {
          throw new IllegalArgumentException(msg);
        } else {
          logger.warn(msg);
        }
      } else {
        fieldValues.put(field.name, values.get(0).getValue());
      }
    }
    return Triple.of(fields.get(0).name, body, fieldValues);
  }

  /**
   * Mapping as a Map, which serializes as the JSON that Elasticsearch expects.
   */
  public Map<String, Object> asMap() {
    return mapping;
  }

  public static void main(String[] args) throws IOException, ParsingException {
    Document doc = XmlParser.fromString("<foo bar='baz'>bar <x/> baz</foo>");
    Nodes x = doc.query("/foo");
    System.out.println(x.get(0).getValue());
  }
}
