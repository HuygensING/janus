package nl.knaw.huygens.pergamon.janus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
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

  AnnotationsResource(Backend backend) {
    this.backend = backend;
  }

  @GET
  @Path("{id}")
  @ApiOperation(value = "Gets an annotation by id",
    response = Annotation.class)
  @ApiResponses(value = {
    @ApiResponse(code = 404, message = "Annotation not found")
  })
  public Response get(@PathParam("id") String id) {
    return Backend.asResponse(backend.getAnnotation(id));
  }

  @POST
  @Path("{id}/annotations")
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Add an annotation on an annotation", response = Backend.PutResult.class)
  public Response putAnnotation(@PathParam("id") String id, Annotation ann) throws IOException {
    return backend.putAnnotation(id, ann).asResponse();
  }

}
