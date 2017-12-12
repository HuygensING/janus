package nl.knaw.huygens.pergamon.janus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static nl.knaw.huygens.pergamon.janus.Identifier.requireValid;

/**
 * Storage of (XML) originals.
 * <p>
 * XML files are stored in a hierarchy of directories. The path to each file is
 * $dir/$hash0/$hash1/$id, where $hash0 and $hash1 are the first two bytes of $id's
 * SHA-256, in lowercase hexadecimal (two characters each).
 * <p>
 * Clients should obtain Writers to update files in the hierarchy.
 * These (when used in try-with-resources blocks) ensure that parts of the
 * hierarchy are properly locked.
 */
public class OriginalStore {
  private static final Logger LOG = LoggerFactory.getLogger(ElasticBackend.class);

  private final Path dir;
  // one lock per $hash0/$hash1
  private final ReadWriteLock locks[] = new ReentrantReadWriteLock[256 * 256];
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

    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }

  public byte[] get(String id) throws IOException, TimeoutException {
    try (LockedReader rd = new LockedReader(id)) {
      return rd.get();
    }
  }

  // Base class of LockedReader and Writer.
  private class BaseReader {
    final int hash;
    final String id;
    final Path path;

    private BaseReader(String id, int hash) {
      requireValid(id);

      this.hash = hash;
      this.id = id;
      path = getPath(id, hash);
    }

    byte[] get() throws IOException, TimeoutException {
      return Files.readAllBytes(path);
    }
  }

  private class LockedReader extends BaseReader implements AutoCloseable {
    private LockedReader(String id) throws TimeoutException {
      super(id, hash(id));

      try {
        if (locks[hash].readLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
          return;
        }
      } catch (InterruptedException e) {
        // Proceed and return TimeoutException.
      }
      throw new TimeoutException("could not get lock for " + getParent(hash));
    }

    @Override
    public void close() {
      locks[hash].readLock().unlock();
    }
  }

  /**
   * A Writer is used to write to/delete the original contents of a specific id.
   */
  public class Writer extends BaseReader implements AutoCloseable {
    private final boolean overwrite;
    private Path tmp = null;

    private Writer(String id, boolean overwrite) throws FileAlreadyExistsException, TimeoutException {
      super(id, hash(id));
      this.overwrite = overwrite;

      try {
        if (locks[hash].writeLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
          return;
        }
      } catch (InterruptedException e) {
        // Proceed and return TimeoutException.
      }
      throw new TimeoutException("could not get lock for " + getParent(hash));
    }

    @Override
    public void close() throws IOException {
      if (tmp != null) {
        Files.move(tmp, path, REPLACE_EXISTING);
      }
      locks[hash].writeLock().unlock();
    }

    public void delete() throws IOException {
      Files.delete(getPath(id, hash));
    }

    public byte[] getIfExists() throws IOException, TimeoutException {
      try {
        return get();
      } catch (NoSuchFileException e) {
        return null;
      }
    }

    public void put(String content) throws IOException, TimeoutException {
      if (!overwrite && Files.exists(path)) {
        throw new FileAlreadyExistsException(path.toString());
      }
      Path dir = getParent(hash);
      mkdir(dir.getParent());
      mkdir(dir);
      tmp = Files.createTempFile(dir, ".tmp_", "");

      try (BufferedWriter out = Files.newBufferedWriter(tmp)) {
        out.write(content);
      } catch (Throwable e) {
        Files.delete(tmp);
        tmp = null;
        throw e;
      }
    }
  }

  /**
   * Equivalent to writer(id, false).
   */
  public Writer writer(String id) throws TimeoutException {
    try {
      return writer(id, false);
    } catch (FileAlreadyExistsException e) {
      throw new RuntimeException(e); // can't happen
    }
  }

  public Writer writer(String id, boolean overwrite) throws FileAlreadyExistsException, TimeoutException {
    return new Writer(id, overwrite);
  }

  private void mkdir(Path dir) throws IOException {
    try {
      Files.createDirectory(dir);
    } catch (FileAlreadyExistsException e) {
      // No problem
    }
  }

  private Path getParent(int h) {
    return dir.resolve(String.format("%02x/%02x", (h & 0xFF00) >>> 8, h & 0xFF));
  }

  Path getPath(String id, int h) {
    return getParent(h).resolve(id);
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
}
