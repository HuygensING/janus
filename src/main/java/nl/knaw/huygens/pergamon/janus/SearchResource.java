package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiOperation;
import org.glassfish.jersey.media.multipart.FormDataContentDisposition;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;
import org.glassfish.jersey.media.multipart.FormDataParam;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@Api(SearchResource.PATH)
@Path(SearchResource.PATH)
@Produces(MediaType.APPLICATION_JSON)
public class SearchResource {
  static final String PATH = "search";

  private static final Logger LOG = LoggerFactory.getLogger(SearchResource.class);

  private final Client client;
  private final String topModUri;

  SearchResource(Client topModClient, String topModUri) {
    this.client = topModClient;
    this.topModUri = topModUri;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Gets search term suggestions based on an input string")
  public Response suggest(SuggestParams params) {
    LOG.debug("params: {}", params);

    final WebTarget target = client.target(topModUri).path("suggest");

    final Entity<SuggestParams> entity = Entity.entity(params, MediaType.APPLICATION_JSON_TYPE);

    return target.request(MediaType.APPLICATION_JSON_TYPE).post(entity);
  }

  @POST
  @Consumes(MediaType.MULTIPART_FORM_DATA)
  @Path("model")
  public Response importModel(@FormDataParam("file") InputStream stream,
                              @FormDataParam("file") FormDataContentDisposition header) {
    LOG.debug("importing Model: {}", header.getFileName());
    LOG.debug("header: {}", header);

    final StreamDataBodyPart dataBodyPart = new StreamDataBodyPart("file", stream);
    final FormDataMultiPart formDataMultiPart = new FormDataMultiPart();
    final MultiPart multiPart = formDataMultiPart.bodyPart(dataBodyPart);
    WebTarget target = client.target(topModUri).path("models").register(MultiPartFeature.class);

    MediaType mediaType = multiPart.getMediaType();
    LOG.debug("multiPart.getMediaType: {}", mediaType);
    final Map<String, String> hackedParams = new HashMap<>(mediaType.getParameters());
    hackedParams.put("boundary", "MyHackedBoundary");
    MediaType hackedMediaType = new MediaType(mediaType.getType(), mediaType.getSubtype(), hackedParams);

    LOG.debug("hackedMediaType: {}", hackedMediaType);

    return target.request().post(Entity.entity(multiPart, hackedMediaType));
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
