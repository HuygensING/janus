package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

@Path("about")
public class AboutResource {
  private static final Logger LOG = LoggerFactory.getLogger(AboutResource.class);

  @JsonProperty
  public final String serviceName;

  @JsonProperty
  public final String startedAt;

  @JsonProperty
  public final Properties buildProperties;

  private final Client client;
  private final String topModUri;
  private final String esHost;

  AboutResource(String serviceName, Properties buildProperties, Client client, String topModUri,
                List<String> esHosts) {
    this.serviceName = serviceName;
    this.client = client;
    this.topModUri = topModUri;
    this.startedAt = Instant.now().toString();
    this.buildProperties = buildProperties;
    this.esHost = esHosts.stream().findFirst().orElse("http://localhost:9200");
    LOG.debug("esHosts: {}, esHost: {}", esHosts, esHost);
  }

  @JsonProperty
  public JsonNode getDependencies() throws IOException {
    final ObjectMapper mapper = Jackson.newMinimalObjectMapper();

    final ObjectNode deps = mapper.createObjectNode();
    deps.set("topmod", mapper.readTree(client.target(topModUri).path("about").request().get(InputStream.class)));
    deps.set("elastic", mapper.readTree(client.target("http://" + esHost).path("/").request().get(InputStream.class)));

    return deps;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {
    return Response.ok(this).build();
  }

}
