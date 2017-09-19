package nl.knaw.huygens.pergamon.janus;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

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
import java.io.UnsupportedEncodingException;

@Api(DocumentsResource.PATH)
@Path(DocumentsResource.PATH)
@Produces(MediaType.APPLICATION_JSON)
public class DocumentsResource {
  static final String PATH = "documents";

  private static final String DOCUMENT_ID = "document ID";
  private static final String XML_NOTES =
    "If the Content-Type is application/xml, it will be broken into text + one annotation per tag (see README).";

  private final RestResponseBuilder responseBuilder = new RestResponseBuilder(PATH);

  private final Backend backend;

  DocumentsResource(Backend backend) {
    this.backend = backend;
  }

  @GET
  @ApiOperation(value = "List of document ids in the index",
    notes = "Paginated; counting starts at 0. Parameter q expects Lucene query syntax.",
    response = Backend.ListPage.class)
  public Response index(@QueryParam("q") String query,
                        @QueryParam("from") @DefaultValue("0") int from,
                        @QueryParam("total") @DefaultValue("100") int count) {
    return Backend.asResponse(backend.listDocs(query, from, count));
  }

  @GET
  @Path("{id}")
  @ApiOperation(value = "Gets a document and its annotations by id",
    response = DocAndAnnotations.class)
  @ApiResponses(value = {
    @ApiResponse(code = 404, message = "Document not found")
  })
  public Response get(@ApiParam(DOCUMENT_ID) @PathParam("id") String id,
                      @QueryParam("recursive") @DefaultValue("true") boolean recursive) throws IOException {
    return Backend.asResponse(backend.getWithAnnotations(id, recursive));
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
    // TODO distinguish between id not found (404) and no annotations for id (empty list)
    return Backend.asResponse(backend.getAnnotations(id, query, recursive));
  }

  @POST
  @Path("search")
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Search documents using Elasticsearch",
    notes = "Returns \"raw\" Elasticsearch results")
  public Response query(String query) throws IOException {
    ElasticBackend eb = (ElasticBackend) backend;
    HttpClient client = HttpClients.createDefault();
    // TODO should not hardcode the host and port.
    String url = String.format("http://localhost:9200/%s/%s/_search", eb.documentIndex, eb.documentType);
    HttpPost req = new HttpPost(url);

    try {
      req.setEntity(new StringEntity(query));
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }

    HttpResponse r = client.execute(req);
    return Response.status(r.getStatusLine().getStatusCode()).entity(r.getEntity().getContent()).build();
  }

  @POST
  @Path("{id}/annotations")
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Add an annotation to a specific document", response = Backend.PutResult.class)
  public Response putAnnotation(@ApiParam(DOCUMENT_ID) @PathParam("id") String id, Annotation ann) throws IOException {
    return responseBuilder.forResult(backend.putAnnotation(id, ann)).build();
  }

  @POST
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Add a document", consumes = "text/plain, application/xml", notes = XML_NOTES)
  public Response putTxt(String content) throws IOException {
    return putTxt(null, content);
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response putJSON(String content) throws IOException {
    return putJSON(null, content);
  }

  @PUT
  @Path("{id}")
  @Consumes(MediaType.TEXT_PLAIN)
  @ApiOperation(value = "Add a document with a specific id", notes = XML_NOTES,
    consumes = "text/plain, application/xml")
  public Response putTxt(@PathParam("id") String id, String content) throws IOException {
    return backend.putTxt(id, content).asResponse();
  }

  @PUT
  @Path("{id}")
  @Consumes(MediaType.APPLICATION_JSON)
  public Response putJSON(@PathParam("id") String id, String content) throws IOException {
    return backend.putJSON(id, content).asResponse();
  }

  @GET
  @Path("{id}/xml")
  @Produces(MediaType.APPLICATION_XML)
  @ApiOperation(value = "Reconstruct XML representation of a document",
    notes = "Only works for documents that were originally uploaded as XML and " +
      "only annotations deriving from tags are included.")
  public Response getXml(@PathParam("id") String id) throws IOException {
    return Backend.asResponse(backend.getXml(id));
  }

  @POST
  @Consumes(MediaType.APPLICATION_XML)
  public Response putXml(String content) throws IOException {
    return responseBuilder.forResult(backend.putXml(null, content)).build();
  }

  @PUT
  @Path("{id}")
  @Consumes(MediaType.APPLICATION_XML)
  public Response putXml(@PathParam("id") String id, String content) throws IOException {
    return backend.putXml(id, content).asResponse();
  }

}
