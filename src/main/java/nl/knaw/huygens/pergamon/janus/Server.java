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
import nl.knaw.huygens.pergamon.janus.xml.XmlParser;
import nu.xom.Document;
import nu.xom.ParsingException;
import org.hibernate.validator.constraints.NotEmpty;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static io.swagger.annotations.SwaggerDefinition.Scheme.HTTP;
import static io.swagger.annotations.SwaggerDefinition.Scheme.HTTPS;
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
  consumes = {MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML},
  schemes = {HTTP, HTTPS}
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
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/")
  public static class Resource {
    private final Backend backend;

    Resource(Backend backend) {
      this.backend = backend;
    }

    @GET
    @Path("documents/{id}")
    @ApiOperation(value = "Gets a document and its annotations by id",
      response = DocAndAnnotations.class)
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
    @Path("documents/{id}/annotations")
    @ApiOperation(value = "Gets the annotations of a specific document by id",
      response = Annotation.class,
      responseContainer = "List"
    )
    public Response getAnnotations(@PathParam("id") String id,
                                   @ApiParam("Recursively get annotations on annotations also")
                                   @QueryParam("recursive") @DefaultValue("true") boolean recursive,
                                   @ApiParam(value = "Lucene style query string")
                                   @QueryParam("q") String query) {
      List<Annotation> result = backend.getAnnotations(id, query, recursive);
      // TODO distinguish between id not found (404) and no annotations for id (empty list)
      if (result.isEmpty()) {
        return Response.status(NOT_FOUND).build();
      }
      return Response.status(OK).entity(result).build();
    }

    @POST
    @Path("documents/{id}/annotations")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Add an annotation to a specific document", response = Backend.PutResult.class)
    public Response putAnnotation(Annotation ann)
      throws IOException {
      return putAnnotation(null, ann);
    }

    @POST
    @Path("annotations/{id}")
    @Consumes(MediaType.APPLICATION_JSON)
    @ApiOperation(value = "Add an annotation to a specific annotation", response = Backend.PutResult.class)
    public Response putAnnotation(@PathParam("id") String id, Annotation ann)
      throws IOException {
      return backend.putAnnotation(ann, id).asResponse();
    }

    @POST
    @Path("documents")
    @Consumes(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Add a document", consumes = "text/plain, application/xml")
    public Response putTxt(String content) throws IOException {
      return putTxt(null, content);
    }

    @PUT
    @Path("documents/{id}")
    @Consumes(MediaType.TEXT_PLAIN)
    @ApiOperation(value = "Add a document with a specific id", consumes = "text/plain, application/xml")
    public Response putTxt(@PathParam("id") String id, String content) throws IOException {
      return backend.putTxt(id, content).asResponse();
    }

    @POST
    @Path("documents")
    @Consumes(MediaType.APPLICATION_XML)
    public Response putXml(String content) throws IOException {
      return putXml(null, content);
    }

    @PUT
    @Path("documents/{id}")
    @Consumes(MediaType.APPLICATION_XML)
    public Response putXml(@PathParam("id") String id, String content) throws IOException {
      try {
        return backend.putXml(id, content).asResponse();
      } catch (ParsingException e) {
        return Response.status(UNSUPPORTED_MEDIA_TYPE).entity(e.toString()).build();
      }
    }

    @POST
    @Consumes(MediaType.APPLICATION_XML)
    @Path("demo/transform")
    public TaggedText transform(String input, @QueryParam("offsets") @DefaultValue("byte") OffsetType offsetType)
      throws ParsingException, IOException {

      return offsetType.transform(XmlParser.fromString(input));
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
  public void run(Config config, Environment environment) throws Exception {
    final Backend backend = new ElasticBackend(config.host, config.documentIndex, config.documentType);
    environment.jersey().register(new Resource(backend));
  }
}
