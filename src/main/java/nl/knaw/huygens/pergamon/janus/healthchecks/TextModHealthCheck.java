package nl.knaw.huygens.pergamon.janus.healthchecks;

import com.codahale.metrics.health.HealthCheck;

import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

public class TextModHealthCheck extends HealthCheck {
  private static final String ABOUT_PATH = "about";

  private final WebTarget target;

  public TextModHealthCheck(WebTarget modeler) {
    this.target = modeler.path(ABOUT_PATH);
  }

  @Override
  protected Result check() {
    final Response response = target.request(MediaType.APPLICATION_JSON_TYPE).get();

    if (response.getStatus() == Response.Status.OK.getStatusCode()) {
      return Result.healthy("GET textmod/%s: %s", ABOUT_PATH, response.getStatusInfo());
    }

    return Result.unhealthy("GET textmod/%s: %s", ABOUT_PATH, response.getStatusInfo());
  }
}
