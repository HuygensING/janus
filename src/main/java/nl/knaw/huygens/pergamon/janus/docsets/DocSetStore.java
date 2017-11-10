package nl.knaw.huygens.pergamon.janus.docsets;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface DocSetStore {
  Collection<DocSet> findAll();

  DocSet createDocSet(Set<String> documentIds);

  Optional<DocSet> findDocSet(UUID uuid);

  DocSet getDocSet(UUID uuid);
}
