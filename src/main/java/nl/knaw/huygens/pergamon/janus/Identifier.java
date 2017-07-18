package nl.knaw.huygens.pergamon.janus;

/**
 * Static utilities for working with identifiers.
 */
public class Identifier {
  // TODO think about case-sensitivity. Elasticsearch produces base64-encoded
  // UUIDs, which are mixed-case.

  // TODO disallow '-' as first char to not confuse Unix tools used naively?

  /**
   * Maximum length of an identifier.
   * <p>
   * This is chosen so that identifiers will be manageable filenames on all common systems.
   * In particular, some Linux filesystems have a maximum length of 144, and we might want to
   * append an extension (e.g., '.json' of five characters).
   */
  public static final int MAX_LENGTH = 139;

  private Identifier() {
  }

  private static final boolean[] validChar = new boolean[128];

  static {
    for (char c = '0'; c <= '9'; c++) {
      validChar[c] = true;
    }
    for (char c = 'a'; c <= 'z'; c++) {
      validChar[c] = true;
    }
    for (char c = '0'; c <= '9'; c++) {
      validChar[c] = true;
    }
    for (char c : new char[]{'_', '-', '.'}) {
      validChar[c] = true;
    }
  }

  /**
   * Determine whether id is a valid identifier.
   * <p>
   * Valid identifiers are strings of up to {@link #MAX_LENGTH} ASCII characters,
   * containing only digits, letters, or the characters "_-.", not ending in a
   * period (".").
   * <p>
   * These restrictions are chosen to produce valid path names on many common
   * platforms.
   */
  public static boolean valid(String id) {
    return id.length() <= MAX_LENGTH && id.chars().allMatch(c -> c < 128 /* ASCII only */ && validChar[c])
      && id.charAt(id.length() - 1) != '.';
  }
}
