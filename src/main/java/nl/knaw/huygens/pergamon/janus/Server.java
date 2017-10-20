package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.client.JerseyClientBuilder;
import io.dropwizard.client.JerseyClientConfiguration;
import io.dropwizard.forms.MultiPartBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import io.swagger.annotations.Contact;
import io.swagger.annotations.Info;
import io.swagger.annotations.License;
import io.swagger.annotations.SwaggerDefinition;
import nl.knaw.huygens.pergamon.janus.graphql.GraphQLResource;
import nl.knaw.huygens.pergamon.janus.healthchecks.TopModHealthCheck;
import nl.knaw.huygens.pergamon.janus.logging.RequestLoggingFilter;
import org.glassfish.jersey.logging.LoggingFeature;
import org.hibernate.validator.constraints.NotEmpty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.logging.Level;

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
  private static final Logger LOG = LoggerFactory.getLogger(Server.class);

  static class Config extends Configuration {
    @Valid
    @NotNull
    @JsonProperty
    private JerseyClientConfiguration jerseyClient = new JerseyClientConfiguration();

    @JsonProperty("elasticsearch")
    private ESConfig es;

    @JsonProperty("swagger")
    private SwaggerBundleConfiguration swaggerBundleConfiguration;

    @JsonProperty
    @NotEmpty
    String topModUri;
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

  static class ServiceConfig {
    private String name;

    private String uri;

    @JsonProperty
    String getName() {
      return name;
    }

    @JsonProperty
    String getUri() {
      return uri;
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
    bootstrap.addBundle(new MultiPartBundle());
  }

  @Override
  public String getName() {
    return "janus";
  }

  @Override
  public void run(Config config, Environment environment) throws Exception {
    final Properties buildProperties = extractBuildProperties().orElse(new Properties());

    final String commitHash = extractCommitHash(buildProperties);
    MDC.put("commit_hash", commitHash); // for 'main' Thread
    environment.jersey().register(new RequestLoggingFilter(commitHash));

    environment.jersey().register(new SandboxResource());

    final Backend backend = createBackend(config);
    environment.jersey().register(new AnnotationsResource(backend));
    environment.jersey().register(new DocumentsResource(backend));
    environment.jersey().register(new GraphQLResource(backend));

    final Client jerseyClient = createTopModClient(config, environment);
    environment.jersey().register(new SearchResource(jerseyClient, config.topModUri));
    environment.jersey().register(
      new AboutResource(getName(), buildProperties, jerseyClient, config, (ElasticBackend) backend));
    environment.healthChecks().register("topmod", new TopModHealthCheck(jerseyClient, config.topModUri));

    environment.jersey().register(new LoggingFeature(java.util.logging.Logger.getLogger(getClass().getName()),
      Level.FINE, LoggingFeature.Verbosity.PAYLOAD_ANY, 1024));

    backend.registerHealthChecks(environment.healthChecks());
  }

  private Client createTopModClient(Config config, Environment environment) {
    return new JerseyClientBuilder(environment).using(config.jerseyClient).build(getName());
  }

  private String extractCommitHash(Properties properties) {
    return properties.getProperty("git.commit.id", "NO-GIT-COMMIT-HASH-FOUND");
  }

  private Optional<Properties> extractBuildProperties() {
    final InputStream propertyStream = getClass().getClassLoader().getResourceAsStream("build.properties");
    if (propertyStream == null) {
      LOG.warn("Resource \"build.properties\" not found");
    } else {
      final Properties properties = new Properties();
      try {
        properties.load(propertyStream);

        if (LOG.isDebugEnabled()) {
          LOG.debug("build.properties: {}", properties);
        }

        return Optional.of(properties);
      } catch (IOException e) {
        LOG.warn("Unable to load build.properties: {}", e);
      }
    }

    return Optional.empty();
  }

  private ElasticBackend createBackend(Config config) throws IOException {
    final ElasticBackend backend = new ElasticBackend(config.es.hosts, config.es.documentIndex, config.es.documentType);
    backend.initIndices();
    return backend;
  }

}
