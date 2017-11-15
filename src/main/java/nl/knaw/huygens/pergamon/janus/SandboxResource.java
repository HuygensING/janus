package nl.knaw.huygens.pergamon.janus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import nl.knaw.huygens.pergamon.janus.xml.TaggedBytes;
import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
import nl.knaw.huygens.pergamon.janus.xml.TaggedText;
import nl.knaw.huygens.pergamon.janus.xml.TaggedUtf16;
import nl.knaw.huygens.pergamon.janus.xml.XmlParser;
import nu.xom.Element;
import nu.xom.ParsingException;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.util.function.Function;

@Api("sandbox")
@Path("/sandbox")
public class SandboxResource {
  private enum OffsetType {
    BYTE(TaggedBytes::new),
    UTF16(TaggedUtf16::new),
    CODEPOINT(TaggedCodepoints::new);

    private final Function<Element, TaggedText> transformer;

    public static OffsetType fromString(String type) {
      return OffsetType.valueOf(type.toUpperCase());
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase();
    }

    OffsetType(Function<Element, TaggedText> transformer) {
      this.transformer = transformer;
    }

    TaggedText transform(Element document) {
      return transformer.apply(document);
    }
  }

  private static final String XML_NOTES =
    "The document will be broken into text + one annotation per tag (see README). " +
      "Nothing is stored. All identifiers in the result are fake.";

  @POST
  @Consumes(MediaType.APPLICATION_XML)
  @Produces(MediaType.APPLICATION_JSON)
  @Path("transformxml")
  @ApiOperation(value = "Perform XML transformation", notes = XML_NOTES)
  public TaggedText transformXml(String input, @QueryParam("offsets") @DefaultValue("byte") OffsetType offsetType)
    throws ParsingException, IOException {

    return offsetType.transform(XmlParser.fromString(input).getRootElement());
  }
}
