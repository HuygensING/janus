package nl.knaw.huygens.pergamon.janus.docsets;

import com.google.common.base.MoreObjects;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

/**
 * Set of document IDs
 */
public interface DocSet {
  UUID getId();

  Set<String> getDocIds();

  boolean addDocument(String id);

  static DocSet fromDocumentIds(Set<String> documentIds) {
    final UUID id = UUID.randomUUID();

    return new DocSet() {
      private final Set<String> documents = documentIds;

      @Override
      public UUID getId() {
        return id;
      }

      @Override
      public Set<String> getDocIds() {
        return Collections.unmodifiableSet(documentIds);
      }

      @Override
      public boolean addDocument(String id) {
        return documentIds.add(id);
      }

      @Override
      public String toString() {
        return MoreObjects.toStringHelper(DocSet.class)
                          .add("id", getId())
                          .add("documents", getDocIds())
                          .toString();
      }
    };
  }
}
