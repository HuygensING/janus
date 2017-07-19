package nl.knaw.huygens.pergamon.janus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import nu.xom.ParsingException;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.OK;

@Api(DocumentsResource.PATH)
@Path(DocumentsResource.PATH)
@Produces(MediaType.APPLICATION_JSON)
public class DocumentsResource {
  static final String PATH = "documents";

  private final RestResponseBuilder responseBuilder = new RestResponseBuilder(PATH);

  private final Backend backend;

  DocumentsResource(Backend backend) {
    this.backend = backend;
  }

  @GET
  @Path("{id}")
  @ApiOperation(value = "Gets a document and its annotations by id",
    response = DocAndAnnotations.class)
  @ApiResponses(value = {
    @ApiResponse(code = 404, message = "Document not found")
  })
  public Response get(@ApiParam(value = "document ID") @PathParam("id") String id) throws IOException {
    DocAndAnnotations result = backend.getWithAnnotations(id, true);
    if (result == null) {
      return Response.status(NOT_FOUND).build();
    }
    return Response.status(OK).entity(result).build();
  }

  @GET
  @Path("{id}/annotations")
  @ApiOperation(value = "Gets the annotations of a specific document by id",
    response = Annotation.class,
    responseContainer = "List"
  )
  public Response getAnnotations(@PathParam("id") String id,
                                 @ApiParam("Recursively get annotations on annotations also")
                                 @QueryParam("recursive") @DefaultValue("true") boolean recursive,
                                 @ApiParam(value = "Lucene style query string")
                                 @QueryParam("q") String query) {
    List<Annotation> result = backend.getAnnotations(id, query, recursive);
    // TODO distinguish between id not found (404) and no annotations for id (empty list)
    if (result.isEmpty()) {
      return Response.status(NOT_FOUND).build();
    }
    return Response.status(OK).entity(result).build();
  }

  @POST
  @Path("{id}/annotations")
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Add an annotation to a specific document", response = Backend.PutResult.class)
  public Response putAnnotation(Annotation ann) throws IOException {
    if (ann.id != null) {
      throw new IllegalArgumentException("annotation may not determine its own id");
    }
    return responseBuilder.forResult(backend.putAnnotation(ann)).build();
  }

  @POST
  @Path("/")
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Add a document", consumes = "text/plain, application/xml")
  public Response putTxt(String content) throws IOException {
    return putTxt(null, content);
  }

  @PUT
  @Path("{id}")
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Add a document with a specific id", consumes = "text/plain, application/xml")
  public Response putTxt(@PathParam("id") String id, String content) throws IOException {
    return backend.putTxt(id, content).asResponse();
  }

  @POST
  @Path("/")
  @Consumes(MediaType.APPLICATION_XML)
  public Response putXml(String content) throws IOException, URISyntaxException {
    try {
      return responseBuilder.forResult(backend.putXml(null, content)).build();
    } catch (ParsingException e) {
      return Response.status(BAD_REQUEST).entity(e.toString()).build();
    }
  }

  @PUT
  @Path("{id}")
  @Consumes(MediaType.APPLICATION_XML)
  public Response putXml(@PathParam("id") String id, String content) throws IOException {
    try {
      return backend.putXml(id, content).asResponse();
    } catch (ParsingException e) {
      return Response.status(BAD_REQUEST).entity(e.toString()).build();
    }
  }

}
