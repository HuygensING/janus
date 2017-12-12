package nl.knaw.huygens.pergamon.janus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static nl.knaw.huygens.pergamon.janus.Identifier.requireValid;

/**
 * Storage of (XML) originals.
 */
public class OriginalStore {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticBackend.class);

  private final Path dir;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();

  public OriginalStore(Path dir) throws IOException {
    try {
      Files.createDirectory(dir);
    } catch (FileAlreadyExistsException e) {
    }
    this.dir = dir;
  }

  public void delete(String id) throws IOException {
    Files.delete(getPath(id));
  }

  // TODO we should have only one of get, getBytes.
  // Responsibilities should be better separated.

  public String get(String id) throws IOException, TimeoutException {
    Path path = getPath(id);
    lockR(id);

    try {
      return new String(Files.readAllBytes(path));
    } finally {
      unlockR(id);
    }
  }

  public Optional<byte[]> getBytes(String id) throws TimeoutException {
    lockR(id);

    try {
      return Optional.of(Files.readAllBytes(getPath(id)));
    } catch (IOException e) {
      LOG.warn("Failed to get original {}: {}", id, e.getMessage());
      return Optional.empty();
    } finally {
      unlockR(id);
    }
  }

  private Path getPath(String id) {
    return dir.resolve(id);
  }

  // TODO: we need more fine-grained locks.

  private void lockR(String id) throws TimeoutException {
    if (!lock.readLock().tryLock()) {
      throw new TimeoutException("could not get lock for " + id);
    }
  }

  private void lockW(String id) throws TimeoutException {
    if (!lock.writeLock().tryLock()) {
      throw new TimeoutException("could not get lock for " + id);
    }
  }

  private void unlockR(String id) {
    lock.readLock().unlock();
  }

  private void unlockW(String id) {
    lock.writeLock().unlock();
  }

  public void put(String id, String content) throws IOException, TimeoutException {
    requireValid(id);
    Path path = getPath(id);

    lockW(id);

    try (BufferedWriter out = Files.newBufferedWriter(path, CREATE_NEW)) {
      out.write(content);
    } catch (Throwable e) {
      Files.delete(path);
      throw e;
    } finally {
      unlockW(id);
    }
  }
}
