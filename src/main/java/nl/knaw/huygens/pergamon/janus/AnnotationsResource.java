package nl.knaw.huygens.pergamon.janus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.List;

@Api(AnnotationsResource.PATH)
@Path(AnnotationsResource.PATH)
@Produces(MediaType.APPLICATION_JSON)
public class AnnotationsResource {
  static final String PATH = "annotations";

  private final RestResponseBuilder responseBuilder = new RestResponseBuilder(PATH);

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

  @GET
  @Path("{id}/annotations")
  @ApiOperation(value = "Get all annotations on a given annotation",
    response = Annotation.class, responseContainer = "List")
  public List<Annotation> getAnnotations(@PathParam("id") String id,
                                         @ApiParam("recursively get annotation on the annotations")
                                         @QueryParam("recursive") @DefaultValue("true") boolean recursive) {
    return backend.getAnnotations(id, null, recursive);
  }

  @POST
  @Path("{id}/annotations")
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Add an annotation on an annotation", response = Backend.PutResult.class)
  public Response putAnnotation(@PathParam("id") String id, Annotation ann) throws IOException {
    return responseBuilder.forResult(backend.putAnnotation(id, ann)).build();
  }

}
