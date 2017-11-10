package nl.knaw.huygens.pergamon.janus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.MoreObjects;
import io.swagger.annotations.Api;
import nl.knaw.huygens.pergamon.janus.docsets.DocSet;
import nl.knaw.huygens.pergamon.janus.docsets.DocSetStore;
import nu.xom.Document;
import nu.xom.Element;
import nu.xom.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;

import java.net.URI;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@Api(DocSetsResource.PATH)
@Path(DocSetsResource.PATH)
@Produces(MediaType.APPLICATION_JSON)
public class DocSetsResource {
  static final String PATH = "docsets";

  private static final Logger LOG = LoggerFactory.getLogger(DocSetsResource.class);

  private final ElasticBackend documentStore;
  private final DocSetStore docSetStore;
  private final WebTarget coCiTarget;

  DocSetsResource(ElasticBackend documentStore, DocSetStore docSetStore, WebTarget coCiTarget) {
    this.documentStore = documentStore;
    this.docSetStore = docSetStore;
    this.coCiTarget = coCiTarget;
  }

  @GET
  @Path("{id}")
  public DocSet findDocSet(@PathParam("id") UUID docSetId) {
    LOG.warn("Getting docSet: {}", docSetId);
    return docSetStore.findDocSet(docSetId).orElseThrow(notFound(docSetId));
  }

  @POST
  @Consumes(MediaType.APPLICATION_JSON)
  public Response createDocSet(Set<String> documentIds) {
    final DocSet docSet = docSetStore.createDocSet(documentIds);
    LOG.debug("generatedId: {}", docSet.getId());

    final URI location = buildLocationUri(docSet.getId());
    LOG.debug("Location: {}", location);

    return Response.created(location).build();
  }

  @GET
  @Path("{id}/co-citations")
  public Response getCoCitations(@PathParam("id") UUID docSetId) {
    Set<XmlDocument> docs = docSetStore.findDocSet(docSetId).orElseThrow(notFound(docSetId)).getDocIds()
                                       .parallelStream()
                                       .map(this::fetchAsXmlDocument)
                                       .filter(Optional::isPresent).map(Optional::get) // Optional::stream in Java 9
                                       .collect(Collectors.toSet());

    return calcCoCitations(docs);
  }

  private Optional<XmlDocument> fetchAsXmlDocument(String id) {
    return documentStore.findDocument(id)
                        .map(this::toXML)
                        .map(xml -> new XmlDocument(id, xml));
  }

  private Response calcCoCitations(Set<XmlDocument> docs) {
    final Entity<XmlDocuments> entity = Entity.entity(new XmlDocuments(docs), MediaType.APPLICATION_JSON_TYPE);

    LOG.debug("Posting entity to /cocit: {}", entity);

    return coCiTarget.path("cocit")
                     .request(MediaType.APPLICATION_JSON_TYPE)
                     .post(entity);
  }

  private Supplier<NotFoundException> notFound(UUID docSetId) {
    return () -> new NotFoundException(String.format("No document set found with id: %s", docSetId));
  }

  private URI buildLocationUri(UUID id) {
    return UriBuilder.fromPath(PATH).path("{id}").build(id);
  }

  private String toXML(String body) {
    final Element root = new Element("hack");
    root.appendChild(new Text(body));
    return new Document(root).toXML();
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

  static class XmlDocuments {
    @JsonProperty
    final String id;
    @JsonProperty
    final Set<XmlDocument> documents;

    private XmlDocuments(Set<XmlDocument> documents) {
      this.id = UUID.randomUUID().toString();
      this.documents = documents;
    }

    @Override
    public String toString() {
      return MoreObjects.toStringHelper(XmlDocuments.class)
                        .add("id", id)
                        .add("documents", documents)
                        .toString();
    }
  }
}
