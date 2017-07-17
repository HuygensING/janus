package nl.knaw.huygens.pergamon.janus.xml;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import nl.knaw.huygens.pergamon.janus.Annotation;

/**
 * A Tag represents an XML element, one of the types of annotations that we support.
 */
public class Tag extends Annotation {
  // Id of Tag that represents the parent element in the XML tree.
  // We store these so that we can reconstruct the tree unambiguously.
  // The combination of Jackson annotations should ensure that no field
  // for this property should ever appear in an API response.
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @JsonProperty(access = JsonProperty.Access.READ_ONLY)
  public final String xmlParent;

  public Tag() {
    super();
    xmlParent = null;
  }

  public Tag(String id, String tag, int start, int end, String target, String xmlParent) {
    super(start, end, target, tag, "", "tag", id);
    this.xmlParent = xmlParent;
  }
}
