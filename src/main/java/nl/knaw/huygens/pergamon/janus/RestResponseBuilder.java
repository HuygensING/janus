package nl.knaw.huygens.pergamon.janus;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

class RestResponseBuilder {
  private final UriBuilder uriBuilder;
  private ElasticBackend.PutResult result;

  RestResponseBuilder(String base) {
    uriBuilder = UriBuilder.fromPath(base).path("{id}");
  }

  RestResponseBuilder forResult(ElasticBackend.PutResult result) {
    this.result = result;
    return this;
  }

  Response build() {
    if (result.status == Response.Status.CREATED.getStatusCode()) {
      return Response.created(uriBuilder.build(result.id)).entity(result).build();
    }

    return result.asResponse();
  }
}
