package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.dropwizard.jackson.Jackson;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.Properties;

@Path("about")
public class AboutResource {
  @JsonProperty
  public final String serviceName;

  @JsonProperty
  public final String startedAt;

  @JsonProperty
  public final Properties buildProperties;

  private final WebTarget topModTarget;

  AboutResource(String serviceName, Properties buildProperties, WebTarget topModTarget) {
    this.serviceName = serviceName;
    this.topModTarget = topModTarget;
    this.startedAt = Instant.now().toString();
    this.buildProperties = buildProperties;
  }

  @JsonProperty
  public JsonNode getDependencies() throws IOException {
    final ObjectMapper mapper = Jackson.newMinimalObjectMapper();

    final InputStream topmod = topModTarget.path("about").request().get(InputStream.class);
    final ObjectNode deps = mapper.createObjectNode();
    deps.set("topmod", mapper.readTree(topmod));
    deps.put("elastic", "TODO");

    return deps;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response get() {
    return Response.ok(this).build();
  }

}
