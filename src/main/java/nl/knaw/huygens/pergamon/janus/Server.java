package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import io.swagger.annotations.Api;
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
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.function.Function;

import static io.swagger.annotations.SwaggerDefinition.Scheme.HTTP;
import static io.swagger.annotations.SwaggerDefinition.Scheme.HTTPS;

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
  static class Config extends Configuration {
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

  @Api("demo")
  @Path("/demo")
  static class DemoResource {
    @POST
    @Consumes(MediaType.APPLICATION_XML)
    @Path("transform")
    public TaggedText transform(String input, @QueryParam("offsets") @DefaultValue("byte") Server.OffsetType offsetType)
      throws ParsingException, IOException {

      return offsetType.transform(XmlParser.fromString(input));
    }
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
    final Backend backend = createBackend(configuration);
    environment.jersey().register(new AnnotationsResource(backend));
    environment.jersey().register(new DocumentsResource(backend));
  }

  private ElasticBackend createBackend(Config configuration) throws UnknownHostException {
    return new ElasticBackend(configuration.host, configuration.documentIndex, configuration.documentType);
  }
}
