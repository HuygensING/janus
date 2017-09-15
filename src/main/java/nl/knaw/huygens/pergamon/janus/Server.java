package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import io.swagger.annotations.Contact;
import io.swagger.annotations.Info;
import io.swagger.annotations.License;
import io.swagger.annotations.SwaggerDefinition;
import nl.knaw.huygens.pergamon.janus.graphql.GraphQLResource;
import nl.knaw.huygens.pergamon.janus.logging.RequestLoggingFilter;
import org.hibernate.validator.constraints.NotEmpty;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.List;

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
    @JsonProperty("elasticsearch")
    private ESConfig es;

    @JsonProperty("swagger")
    private SwaggerBundleConfiguration swaggerBundleConfiguration;
  }

  static class ESConfig {
    @JsonProperty
    private List<String> hosts;

    @JsonProperty
    @NotEmpty
    private String documentIndex;

    @JsonProperty
    @NotEmpty
    private String documentType;
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
    environment.jersey().register(new RequestLoggingFilter());

    environment.jersey().register(new SandboxResource());

    final Backend backend = createBackend(configuration);
    environment.jersey().register(new AnnotationsResource(backend));
    environment.jersey().register(new DocumentsResource(backend));
    environment.jersey().register(new GraphQLResource(backend));

    backend.registerHealthChecks(environment.healthChecks());
  }

  private ElasticBackend createBackend(Config config) throws IOException {
    final ElasticBackend backend = new ElasticBackend(config.es.hosts, config.es.documentIndex, config.es.documentType);
    if (!backend.initIndices()) {
      throw new RuntimeException("Failed to initialize elastic search indices");
    }
    return backend;
  }
}
