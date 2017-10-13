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
import java.util.List;
import java.util.Properties;

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

  private final Client webClient;
  private final List<Server.ServiceConfig> services;

  AboutResource(String serviceName, Properties buildProperties, Client webClient, List<Server.ServiceConfig> services) {
    this.serviceName = serviceName;
    this.buildProperties = buildProperties;
    this.webClient = webClient;
    this.services = services;
    this.startedAt = Instant.now().toString();
  }

  @JsonProperty
  public JsonNode getServices() {
    final ObjectMapper mapper = Jackson.newMinimalObjectMapper();
    final ArrayNode links = mapper.createArrayNode();

    this.services.forEach(service -> {
      final ObjectNode node = mapper.createObjectNode();
      try {
        links.add(node.set(service.getName(), mapper.readTree(getAbout(service))));
      } catch (IOException e) {
        links.add(node.put(service.getName(), String.format("%s failed: %s", service.getUri(), e.getMessage())));
      }
    });

    return links;
  }

  private InputStream getAbout(Server.ServiceConfig service) {
    return webClient.target(service.getUri()).request(MediaType.APPLICATION_JSON_TYPE).get(InputStream.class);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {
    return Response.ok(this).build();
  }

}
