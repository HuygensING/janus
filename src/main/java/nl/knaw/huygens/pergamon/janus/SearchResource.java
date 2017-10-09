package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiOperation;
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

@Api(SearchResource.PATH)
@Path(SearchResource.PATH)
@Produces(MediaType.APPLICATION_JSON)
public class SearchResource {
  static final String PATH = "search";

  private static final Logger LOG = LoggerFactory.getLogger(SearchResource.class);

  private final WebTarget target;

  SearchResource(WebTarget target) {
    this.target = target;
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  @ApiOperation(value = "Gets search term suggestions based on an input string")
  public Response suggest(SuggestParams params) {
    LOG.debug("params: {}", params);

    final Entity<SuggestParams> entity = Entity.entity(params, MediaType.APPLICATION_JSON_TYPE);

    return target.request(MediaType.APPLICATION_JSON_TYPE).post(entity);
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
