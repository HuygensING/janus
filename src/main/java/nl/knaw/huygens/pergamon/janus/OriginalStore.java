package nl.knaw.huygens.pergamon.janus;

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

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
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

  private class LockedReader implements AutoCloseable {
    final int hash;
    final String id;
    final Path path;

    private LockedReader(String id) throws TimeoutException {
      requireValid(id);
      this.hash = hash(id);
      this.id = id;
      path = getPath(id, hash);

      try {
        if (locks[hash].readLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
          return;
        }
      } catch (InterruptedException e) {
        // Proceed to throw TimeoutException.
      }
      throw new TimeoutException("could not get lock for " + getParent(hash));
    }

    @Override
    public void close() {
      locks[hash].readLock().unlock();
    }

    byte[] get() throws IOException, TimeoutException {
      return Files.readAllBytes(path);
    }
  }

  public abstract class WriteOp implements AutoCloseable {
    protected final int hash;
    protected Path path;

    private WriteOp(String id) throws TimeoutException {
      requireValid(id);
      this.hash = hash(id);
      path = getPath(id, hash);

      try {
        if (locks[hash].writeLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
          return;
        }
      } catch (InterruptedException e) {
        // Throw TimeoutException below.
      }
      throw new TimeoutException("could not get lock for " + getParent(hash));
    }

    @Override
    public void close() throws IOException {
      locks[hash].writeLock().unlock();
    }

    /**
     * A WriteOp doesn't take place until it is committed.
     */
    public abstract void commit() throws IOException;

    /**
     * Checks if there is any work to be done for this WriteOp.
     * <p>
     * This can be used to check whether an original exists before deletion
     * or has the desired contents for a replace. If it returns true, no ES
     * queries need to be performed.
     */
    public abstract boolean noop() throws IOException;
  }

  private class Delete extends WriteOp {
    private Delete(String id) throws TimeoutException {
      super(id);
    }

    @Override
    public void commit() throws IOException {
      Files.delete(path);
      path = null; // guard against multiple commit
    }

    @Override
    public boolean noop() {
      return !Files.exists(path);
    }
  }

  private class Put extends WriteOp {
    private final String content;
    private final boolean overwrite;

    private Put(String id, String content, boolean overwrite) throws TimeoutException {
      super(id);
      this.content = content;
      this.overwrite = overwrite;
    }

    @Override
    public void commit() throws IOException {
      if (!overwrite && Files.exists(path)) {
        throw new FileAlreadyExistsException(path.toString());
      }

      Path dir = getParent(hash);
      mkdir(dir.getParent());
      mkdir(dir);
      Path tmp = Files.createTempFile(dir, ".tmp_", "");

      try (BufferedWriter out = Files.newBufferedWriter(tmp)) {
        out.write(content);
        Files.move(tmp, path, ATOMIC_MOVE, REPLACE_EXISTING);
      } catch (Throwable e) {
        Files.delete(tmp);
        throw e;
      }
    }

    @Override
    public boolean noop() throws IOException {
      try {
        return overwrite && new String(Files.readAllBytes(path)).equals(content);
      } catch (NoSuchFileException e) {
        return false;
      }
    }
  }

  public WriteOp delete(String id) throws TimeoutException {
    return new Delete(id);
  }

  public WriteOp put(String id, String content) throws TimeoutException {
    return new Put(id, content, false);
  }

  public WriteOp replace(String id, String content) throws TimeoutException {
    return new Put(id, content, true);
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
