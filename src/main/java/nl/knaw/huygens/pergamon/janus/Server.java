package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.ParsingException;
import org.hibernate.validator.constraints.NotEmpty;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.StringReader;
import java.net.UnknownHostException;
import java.util.function.Function;

/**
 * Web server.
 */
public class Server extends Application<Server.Config> {
  public static class Config extends Configuration {
    @JsonProperty
    @NotEmpty
    private String documentIndex;

    @JsonProperty
    @NotEmpty
    private String documentType;
  }

  private enum OffsetType {
    BYTE(TaggedBytes::new),
    UTF16(TaggedUtf16::new),
    CODEPOINT(TaggedCodepoints::new);

    private final Function<Document, TaggedText> transformer;

    public static OffsetType fromString(String type) {
      return OffsetType.valueOf(type.toUpperCase());
    }

    OffsetType(Function<Document, TaggedText> transformer) {
      this.transformer = transformer;
    }

    TaggedText transform(Document document) {
      return transformer.apply(document);
    }
  }

  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public static class Resource {
    private final Backend backend;
    private static Builder parser = new Builder();

    public Resource(Config config) throws UnknownHostException {
      backend = new ElasticBackend(config.documentIndex, config.documentType);
    }

    @Path("/get/{id}")
    @GET
    public Object get(@PathParam("id") String id) throws IOException {
      Object result = backend.getWithAnnotations(id);
      if (result == null) {
        // TODO: improve error handling
        result = 404;
      }
      return result;
    }

    @Consumes("application/json")
    @Path("/annotate/{target}/{id}")
    @POST
    public int putAnnotation(@PathParam("target") String target, @PathParam("id") String id, Annotation ann)
      throws IOException {
      return backend.putAnnotation(ann, id, target);
    }

    @Path("/putxml/{id}")
    @POST
    public int putXml(@PathParam("id") String id, String content) throws IOException, ParsingException {
      return backend.putXml(id, parser.build(new StringReader(content)));
    }

    @Path("/transform")
    @POST
    public TaggedText transform(String input, @QueryParam("offsets") @DefaultValue("byte") OffsetType offsetType)
      throws ParsingException, IOException {

      return offsetType.transform(parser.build(new StringReader(input)));
    }
  }

  public static void main(String[] args) throws Exception {
    new Server().run(args);
  }

  @Override
  public String getName() {
    return "janus";
  }

  @Override
  public void run(Config configuration, Environment environment) throws Exception {
    environment.jersey().register(new Resource(configuration));
  }
}
