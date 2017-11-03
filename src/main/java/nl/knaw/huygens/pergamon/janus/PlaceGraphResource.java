package nl.knaw.huygens.pergamon.janus;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.IOException;

@Deprecated
@Path("/placegraph")
@Produces(MediaType.APPLICATION_JSON)
public class PlaceGraphResource {
  private final ElasticBackend backend;

  public PlaceGraphResource(ElasticBackend backend) {
    this.backend = backend;
  }

  @POST
  public Response getGraph(String filter) throws IOException {
    try {
      return Response.status(200).entity(backend.placeGraph(filter)).build();
    } catch (Throwable e) {
      return Response.status(500).entity(e.getMessage()).build();
    }
  }
}
