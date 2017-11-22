package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.MoreObjects;
import io.swagger.annotations.Api;
import nl.knaw.huygens.pergamon.janus.docsets.DocSet;
import nl.knaw.huygens.pergamon.janus.docsets.DocSetStore;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Text;
import org.apache.http.HttpEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static nl.knaw.huygens.pergamon.janus.DocSetsResource.CoCitationFormat.FORMAT_PARAM_NAME;

@Api(DocSetsResource.PATH)
@Path(DocSetsResource.PATH)
@Produces(MediaType.APPLICATION_JSON)
public class DocSetsResource {
  static final String PATH = "docsets";


  private static final Logger LOG = LoggerFactory.getLogger(DocSetsResource.class);

  private final ElasticBackend documentStore;
  private final DocSetStore docSetStore;
  private final WebTarget coCiTarget;
  private final ObjectMapper mapper;

  DocSetsResource(ElasticBackend documentStore, DocSetStore docSetStore, WebTarget coCiTarget) {
    this.documentStore = documentStore;
    this.docSetStore = docSetStore;
    this.coCiTarget = coCiTarget;
    this.mapper = new ObjectMapper();
  }

  @GET
  public Collection<DocSet> getDocSets() {
    return docSetStore.findAll();
  }

  @GET
  @Path("{id}")
  public DocSet findDocSet(@PathParam("id") UUID docSetId) {
    LOG.warn("Getting docSet: {}", docSetId);
    return docSetStore.findDocSet(docSetId).orElseThrow(notFound(docSetId));
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createDocSet(String query) throws IOException {
    final org.elasticsearch.client.Response response = documentStore.search(sanitise(query));
    final Set<String> documentIds = streamHits(response.getEntity()).map(this::extractId).collect(Collectors.toSet());
    final DocSet docSet = docSetStore.createDocSet(documentIds);
    return Response.created(locationOf(docSet)).build();
  }

  private String sanitise(String query) throws IOException {
    final Map dirty = mapper.readValue(query, Map.class);

    // Strip things like 'size: 0' and '_source: XXX'
    final Map<String, Object> clean = new HashMap<>();
    clean.put("query", dirty.get("query"));

    return mapper.writeValueAsString(clean);
  }

  @SuppressWarnings("unchecked")
  private Stream<Map> streamHits(HttpEntity entity) throws IOException {
    final Map map = mapper.readValue(entity.getContent(), Map.class);
    final Map hits = (Map) map.get("hits");
    return ((List<Map>) hits.get("hits")).stream();
  }

  private String extractId(Map hit) {
    return hit.get("_id").toString();
  }

  @GET
  @Path("{id}/cocitations")
  public Response getCoCitations(@PathParam("id") UUID docSetId,
                                 @QueryParam(FORMAT_PARAM_NAME) @DefaultValue("simple") CoCitationFormat format) {
    Set<XmlDocument> docs = docSetStore.findDocSet(docSetId).orElseThrow(notFound(docSetId)).getDocIds()
                                       .parallelStream()
                                       .map(this::fetchAsXmlDocument)
                                       .filter(Optional::isPresent).map(Optional::get) // Optional::stream in Java 9
                                       .collect(Collectors.toSet());

    LOG.debug("Collected {} document(s) for docSet: {}", docs.size(), docSetId);

    return calcCoCitations(docs, format);
  }

  private Optional<XmlDocument> fetchAsXmlDocument(String id) {
    return documentStore.getOriginalBytes(id)
                        .map(String::new)
                        .map(xml -> new XmlDocument(id, xml));
  }

  private Response calcCoCitations(Set<XmlDocument> docs, CoCitationFormat format) {
    final Entity<Set<XmlDocument>> entity = Entity.entity(docs, MediaType.APPLICATION_JSON_TYPE);

    LOG.trace("Posting entity to /cocit: {}", entity);

    return coCiTarget.path("cocit")
                     .queryParam(FORMAT_PARAM_NAME, format)
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .post(entity);
  }

  private Supplier<NotFoundException> notFound(UUID docSetId) {
    return () -> new NotFoundException(String.format("No document set found with id: %s", docSetId));
  }

  private URI locationOf(DocSet docSet) {
    return UriBuilder.fromPath(PATH).path("{id}").build(docSet.getId());
  }

  private String toXML(String body) {
    final Element root = new Element("hack");
    root.appendChild(new Text(body));
    return new Document(root).toXML();
  }

  protected enum CoCitationFormat {
    full, graph, simple;

    static final String FORMAT_PARAM_NAME = "format";
  }

  static class XmlDocument {
    @JsonProperty
    final String id;
    @JsonProperty
    final String xml;

    private XmlDocument(String id, String xml) {
      this.id = id;
      this.xml = xml;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(XmlDocument.class)
                        .add("id", id)
                        .add("xml", xml)
                        .toString();
    }
  }

}
