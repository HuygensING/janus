package nl.knaw.huygens.pergamon.janus;

import io.swagger.annotations.Api;
import nl.knaw.huygens.pergamon.janus.xml.TaggedBytes;
import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
import nl.knaw.huygens.pergamon.janus.xml.TaggedText;
import nl.knaw.huygens.pergamon.janus.xml.TaggedUtf16;
import nl.knaw.huygens.pergamon.janus.xml.XmlParser;
import nu.xom.Document;
import nu.xom.ParsingException;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.function.Function;

@Api("demo")
@Path("/demo")
public class DemoResource {
  private enum OffsetType {
    BYTE(TaggedBytes::new),
    UTF16(TaggedUtf16::new),
    CODEPOINT(TaggedCodepoints::new);

    private final Function<Document, TaggedText> transformer;

    public static OffsetType fromString(String type) {
      return OffsetType.valueOf(type.toUpperCase());
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase();
    }

    OffsetType(Function<Document, TaggedText> transformer) {
      this.transformer = transformer;
    }

    TaggedText transform(Document document) {
      return transformer.apply(document);
    }
  }

  @POST
  @Consumes(MediaType.APPLICATION_XML)
  @Path("transform")
  public TaggedText transform(String input, @QueryParam("offsets") @DefaultValue("byte") OffsetType offsetType)
    throws ParsingException, IOException {

    return offsetType.transform(XmlParser.fromString(input));
  }
}
