package nl.knaw.huygens.pergamon.janus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static nl.knaw.huygens.pergamon.janus.Identifier.requireValid;

/**
 * Storage of (XML) originals.
 * <p>
 * XML files are stored in a hierarchy of directories. The path to each file is
 * $dir/$hash0/$hash1/$id, where $hash0 and $hash1 are the first two bytes of id's
 * SHA-256, in lowercase hexadecimal (two characters each).
 */
public class OriginalStore {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticBackend.class);

  private final Path dir;
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  private final long timeout;

  /**
   * @param dir     Top-level directory.
   * @param timeout Timeout, in milliseconds.
   * @throws IOException
   */
  public OriginalStore(Path dir, long timeout) throws IOException {
    try {
      Files.createDirectory(dir);
    } catch (FileAlreadyExistsException e) {
    }
    this.dir = dir;
    this.timeout = timeout;
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

  public void put(String id, String content) throws IOException, TimeoutException {
    requireValid(id);
    Path path = getPath(id);

    lockW(id);

    try {
      mkdir(path.getParent().getParent());
      mkdir(path.getParent());
      try (BufferedWriter out = Files.newBufferedWriter(path, CREATE_NEW)) {
        out.write(content);
      } catch (Throwable e) {
        Files.delete(path);
        throw e;
      }
    } finally {
      unlockW(id);
    }
  }

  void mkdir(Path dir) throws IOException {
    try {
      Files.createDirectory(dir);
    } catch (FileAlreadyExistsException e) {
      // No problem
    }
  }

  Path getPath(String id) {
    String h = String.format("%04x", hash(id));
    return dir
      //.resolve(String.format("%04x", hash(id)))
      .resolve(h.substring(0, 2))
      .resolve(h.substring(2, 4))
      .resolve(id);
  }

  // First two bytes of SHA-256 of id.
  static int hash(String id) {
    try {
      MessageDigest sha = MessageDigest.getInstance("SHA-256");
      sha.update(id.getBytes("UTF-8"));
      byte[] h = sha.digest();
      int h0 = (int) h[0] & 0xFF;
      int h1 = (int) h[1] & 0xFF;
      return (h0 << 8) | h1;
    } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  // TODO: we need more fine-grained locks.

  private void lockR(String id) throws TimeoutException {
    try {
      if (lock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
        return;
      }
    } catch (InterruptedException e) {
      // Proceed and return TimeoutException.
    }
    throw new TimeoutException("could not get lock for " + id);
  }

  private void lockW(String id) throws TimeoutException {
    try {
      if (lock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
        return;
      }
    } catch (InterruptedException e) {
      // Proceed and return TimeoutException.
    }
    throw new TimeoutException("could not get lock for " + id);
  }

  private void unlockR(String id) {
    lock.readLock().unlock();
  }

  private void unlockW(String id) {
    lock.writeLock().unlock();
  }
}
