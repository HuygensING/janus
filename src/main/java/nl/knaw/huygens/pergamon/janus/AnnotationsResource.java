package nl.knaw.huygens.pergamon.janus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

@Api("annotations")
@Produces(MediaType.APPLICATION_JSON)
@Path("/annotations")
public class AnnotationsResource {
  private final Backend backend;

  public AnnotationsResource(Backend backend) {
    this.backend = backend;
  }

  @POST
  @Path("{id}/annotations")
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Add an annotation to a specific annotation", response = Backend.PutResult.class)
  public Response putAnnotation(@PathParam("id") String id, Annotation ann) throws IOException {
    if (ann.id != null) {
      throw new IllegalArgumentException("annotation may not determine its own id");
    }
    ann.id = id;
    return backend.putAnnotation(ann).asResponse();
  }

}
