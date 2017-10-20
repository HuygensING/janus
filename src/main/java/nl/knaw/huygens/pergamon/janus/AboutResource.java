package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dropwizard.jackson.Jackson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/**
 * Reports information about configuration, versions of dependencies, etc.
 */
@Path("about")
public class AboutResource {
  private static final Logger LOG = LoggerFactory.getLogger(AboutResource.class);

  @JsonProperty
  public final String serviceName;

  @JsonProperty
  public final String startedAt;

  @JsonProperty
  public final Properties buildProperties;

  // Endpoint of a dependency that we query to get information to pass on.
  private interface Dependency {
    InputStream getInfo() throws IOException;
  }

  // Dependency where we can query a base url + "/about".
  private class SimpleDependency implements Dependency {
    private final String url;

    SimpleDependency(String url) {
      this.url = url;
    }

    @Override
    public InputStream getInfo() throws IOException {
      return webClient.target(url + "/about").request(MediaType.APPLICATION_JSON_TYPE).get(InputStream.class);
    }
  }

  private final Client webClient;
  private final ElasticBackend elasticsearch;
  private final Server.Config config;

  /**
   * @param serviceName     Name of this service (probably Janus).
   * @param buildProperties Our build properties.
   * @param webClient       Web client, used to perform requests.
   * @param config          Server configuration.
   * @param elastic         Elasticsearch client.
   */
  AboutResource(String serviceName, Properties buildProperties, Client webClient, Server.Config config,
                ElasticBackend elastic) {
    this.elasticsearch = elastic;
    this.serviceName = serviceName;
    this.buildProperties = buildProperties;
    this.webClient = webClient;
    this.config = config;
    this.startedAt = Instant.now().toString();
  }

  @JsonProperty
  public JsonNode getConfig() {
    final ObjectMapper mapper = Jackson.newMinimalObjectMapper();
    final ArrayNode links = mapper.createArrayNode();

    final Map<String, Dependency> dependencies = new TreeMap<>();

    dependencies.put("elasticsearch", elasticsearch::about);
    dependencies.put("frontendSupplier", new SimpleDependency(config.frontendUri));
    dependencies.put("topmod", new SimpleDependency(config.topModUri));

    dependencies.forEach((name, dependency) -> {
      final ObjectNode node = mapper.createObjectNode();
      try {
        links.add(node.set(name, mapper.readTree(dependency.getInfo())));
      } catch (IOException e) {
        links.add(node.put(name, String.format("failed: %s", e.getMessage())));
      }
    });

    return links;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {
    return Response.ok(this).build();
  }
}
