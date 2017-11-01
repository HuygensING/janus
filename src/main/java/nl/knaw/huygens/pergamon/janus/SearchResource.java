package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiOperation;
import org.glassfish.jersey.media.multipart.Boundary;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.io.InputStream;

@Api(SearchResource.PATH)
@Path(SearchResource.PATH)
@Produces(MediaType.APPLICATION_JSON)
public class SearchResource {
  static final String PATH = "search";

  private static final String MODEL_UPLOAD_PATH = "models";
  private static final String FILE_PARAM = "file";

  private static final Logger LOG = LoggerFactory.getLogger(SearchResource.class);

  private final WebTarget modeler;

  SearchResource(WebTarget modeler) {
    this.modeler = modeler;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Gets search term suggestions based on an input string")
  public Response suggest(SuggestParams params) {
    LOG.debug("params: {}", params);

    final WebTarget target = modeler.path("suggest");

    final Entity<SuggestParams> entity = Entity.entity(params, MediaType.APPLICATION_JSON_TYPE);

    return target.request(MediaType.APPLICATION_JSON_TYPE).post(entity);
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("model")
  public Response importModel(@FormDataParam(FILE_PARAM) InputStream stream) {
    final MultiPart multiPart = new FormDataMultiPart().bodyPart(new StreamDataBodyPart(FILE_PARAM, stream));
    final Entity<MultiPart> entry = workAroundMissingBoundaryInContentTypeHeader(multiPart);
    return modelUploadTarget().request().post(entry);
  }

  @NotNull
  private Entity<MultiPart> workAroundMissingBoundaryInContentTypeHeader(MultiPart multiPart) {
    // If boundary is not explicitly added here, DW fails to set the boundary on the
    // "Content-Type" header (although it *does* set it on the subsequent form parts),
    // sending out a bad HTTP request, which topmod then chokes on.

    // should just be: return Entity.entity(multiPart, multiPart.getMediaType());
    return Entity.entity(multiPart, Boundary.addBoundary(multiPart.getMediaType()));
  }

  private WebTarget modelUploadTarget() {
    return modeler.path(MODEL_UPLOAD_PATH).register(MultiPartFeature.class);
  }

  static class SuggestParams {
    @ApiModelProperty(value = "terms entered by user to seed the suggestion", required = true)
    @JsonProperty
    String query;

    @ApiModelProperty(value = "topic model id to be used for the suggestions")
    @JsonProperty
    String model = "default";

    @ApiModelProperty(value = "max. number of suggestion terms to be returned")
    @JsonProperty
    int maxTerms = 10;

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(this)
                        .add("query", query)
                        .add("model", model)
                        .add("maxTerms", maxTerms)
                        .toString();
    }
  }
}
