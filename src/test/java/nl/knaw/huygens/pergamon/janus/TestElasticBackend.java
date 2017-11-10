package nl.knaw.huygens.pergamon.janus;

import org.apache.http.HttpHost;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;

// Only simple unit tests that don't use a backend belong here.
// The rest goes in TestElasticBackendIntegration.
public class TestElasticBackend {
  @Test
  public void parseAddr() throws UnknownHostException {
    HttpHost addr = ElasticBackend.parseAddr("localhost:9301");
    assertHostEquals("localhost", addr);
    assertEquals(9301, addr.getPort());

    addr = ElasticBackend.parseAddr("localhost");
    assertHostEquals("localhost", addr);
    assertEquals(9200, addr.getPort());

    // IPv6
    addr = ElasticBackend.parseAddr("[::1]:9300");
    assertHostEquals("[::1]", addr);
    assertEquals(9300, addr.getPort());

    addr = ElasticBackend.parseAddr("[::1]");
    assertHostEquals("[::1]", addr);
    assertEquals(9200, addr.getPort());
  }

  private void assertHostEquals(String expected, HttpHost addr) {
    assertEquals(expected, addr.getHostName());
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidPortNumber() throws UnknownHostException {
    ElasticBackend.parseAddr("localhost:0x00");
  }
}
