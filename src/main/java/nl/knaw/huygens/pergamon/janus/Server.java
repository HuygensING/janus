package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import io.swagger.annotations.Contact;
import io.swagger.annotations.Info;
import io.swagger.annotations.License;
import io.swagger.annotations.SwaggerDefinition;
import nl.knaw.huygens.pergamon.janus.xml.TaggedBytes;
import nl.knaw.huygens.pergamon.janus.xml.TaggedCodepoints;
import nl.knaw.huygens.pergamon.janus.xml.TaggedText;
import nl.knaw.huygens.pergamon.janus.xml.TaggedUtf16;
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
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.StringReader;
import java.net.UnknownHostException;
import java.util.List;
import java.util.function.Function;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;
import static javax.ws.rs.core.Response.Status.UNSUPPORTED_MEDIA_TYPE;

/**
 * Web server.
 */
@SwaggerDefinition(
  info = @Info(
    description = "Operations on texts and annotations",
    version = "1.0",
    title = "Janus: Pergamon's API face",
    termsOfService = "http://example.com/to-be-determined.html",
    contact = @Contact(
      name = "Developers",
      email = "janus@example.com",
      url = "http://pergamon.huygens.knaw.nl"
    ),
    license = @License(
      name = "GNU GENERAL PUBLIC LICENSE",
      url = "https://www.gnu.org/licenses/licenses.en.html#GPL"
    )
  ),
  schemes = {SwaggerDefinition.Scheme.HTTP, SwaggerDefinition.Scheme.HTTPS}
)
public class Server extends Application<Server.Config> {
  public static class Config extends Configuration {
    @JsonProperty
    private String host = "localhost";

    @JsonProperty
    @NotEmpty
    private String documentIndex;

    @JsonProperty
    @NotEmpty
    private String documentType;

    @JsonProperty("swagger")
    private SwaggerBundleConfiguration swaggerBundleConfiguration;
  }

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

  @Api(value = "janus")
  @Path("/documents")
  @Produces(MediaType.APPLICATION_JSON)
  public static class Resource {
    private final Backend backend;
    private static final Builder parser = new Builder();

    public Resource(Config config) throws UnknownHostException {
      backend = new ElasticBackend(config.host, config.documentIndex, config.documentType);
    }

    @GET
    @Path("{id}")
    @ApiOperation(value = "Gets a document and its annotations by id",
      response = Annotation.class)
    @ApiResponses(value = {
      @ApiResponse(code = 404, message = "Document not found")
    })
    public Response get(@ApiParam(value = "document ID") @PathParam("id") String id) throws IOException {
      DocAndAnnotations result = backend.getWithAnnotations(id, true);
      if (result == null) {
        return Response.status(NOT_FOUND).build();
      }
      return Response.status(OK).entity(result).build();
    }

    @GET
    @Path("{id}/annotations")
    public Response getAnnotations(@PathParam("id") String id,
                                   @QueryParam("recursive") @DefaultValue("true") boolean recursive,
                                   @QueryParam("q") String query) {
      List<Annotation> result = backend.getAnnotations(id, query, recursive);
      // TODO distinguish between id not found (404) and no annotations for id (empty list)
      if (result.isEmpty()) {
        return Response.status(NOT_FOUND).build();
      }
      return Response.status(OK).entity(result).build();
    }

    @POST
    @Path("/annotate")
    @Consumes("application/json")
    public Response putAnnotation(Annotation ann)
      throws IOException {
      return putAnnotation(null, ann);
    }

    @Consumes("application/json")
    @Path("/annotate/{id}")
    @POST
    public Response putAnnotation(@PathParam("id") String id, Annotation ann)
      throws IOException {
      return backend.putAnnotation(ann, id).asResponse();
    }

    @Path("/put")
    @POST
    public Response putTxt(String content) throws IOException {
      return putTxt(null, content);
    }

    @Path("/put/{id}")
    @POST
    public Response putTxt(@PathParam("id") String id, String content) throws IOException {
      return backend.putTxt(id, content).asResponse();
    }

    @Path("/putxml")
    @POST
    public Response putXml(String content) throws IOException {
      return putXml(null, content);
    }

    @Path("/putxml/{id}")
    @POST
    public Response putXml(@PathParam("id") String id, String content) throws IOException {
      try {
        return backend.putXml(id, parser.build(new StringReader(content))).asResponse();
      } catch (ParsingException e) {
        return Response.status(UNSUPPORTED_MEDIA_TYPE).entity(e.toString()).build();
      }
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
  public void initialize(Bootstrap<Config> bootstrap) {
    bootstrap.addBundle(new SwaggerBundle<Config>() {
      @Override
      protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(Config config) {
        return config.swaggerBundleConfiguration;
      }
    });
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
