package nl.knaw.huygens.pergamon.janus.docsets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class InMemoryDocSetStore implements DocSetStore {
  private static final Logger LOG = LoggerFactory.getLogger(InMemoryDocSetStore.class);

  private final Map<UUID, DocSet> memoryStore = new HashMap<>();

  @Override
  public DocSet createDocSet(Set<String> documentIds) {
    final DocSet docSet = DocSet.fromDocumentIds(documentIds);
    LOG.trace("created docSet: {}", docSet);
    memoryStore.put(docSet.getId(), docSet);
    return docSet;
  }

  @Override
  public DocSet getDocSet(UUID uuid) {
    return memoryStore.get(uuid);
  }

  @Override
  public Optional<DocSet> findDocSet(UUID uuid) {
    return Optional.ofNullable(getDocSet(uuid));
  }
}
