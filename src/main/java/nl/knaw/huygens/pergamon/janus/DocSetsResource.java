package nl.knaw.huygens.pergamon.janus;

import io.swagger.annotations.Api;
import nl.knaw.huygens.pergamon.janus.docsets.DocSet;
import nl.knaw.huygens.pergamon.janus.docsets.DocSetStore;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

@Api(DocSetsResource.PATH)
@Path(DocSetsResource.PATH)
@Produces(MediaType.APPLICATION_JSON)
public class DocSetsResource {
  static final String PATH = "docsets";

  private static final Logger LOG = LoggerFactory.getLogger(DocSetsResource.class);

  private final DocSetStore store;

  DocSetsResource(DocSetStore store) {
    this.store = store;
  }

  @GET
  @Path("{id}")
  public DocSet findDocSet(@PathParam("id") UUID docSetId) {
    LOG.warn("Getting docSet: {}", docSetId);
    return store.findDocSet(docSetId).orElseThrow(notFound(docSetId));
    // final Optional<DocSet> docSet = store.findDocSet(docSetId);
    // LOG.debug("Found docSet: {}", docSet);
    // return Response.ok(docSet).build();
  }

  @NotNull
  private Supplier<NotFoundException> notFound(UUID docSetId) {
    return () -> new NotFoundException(String.format("No document set found with id: %s", docSetId));
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createDocSet(@Context UriInfo uriInfo, Set<String> documentIds) {
    final DocSet docSet = store.createDocSet(documentIds);
    LOG.debug("generatedId: {}", docSet.getId());

    final URI location = buildLocationUri(docSet.getId());
    LOG.debug("Location: {}", location);

    return Response.created(location).build();
  }

  private URI buildLocationUri(UUID id) {
    return UriBuilder.fromPath(PATH).path("{id}").build(id);
  }
}
