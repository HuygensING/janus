package nl.knaw.huygens.pergamon.janus;

import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;

// Only simple unit tests that don't use a backend belong here.
// The rest goes in TestElasticBackendIntegration.
public class TestElasticBackend {
  @Test
  public void parseAddr() throws UnknownHostException {
    InetSocketTransportAddress addr = ElasticBackend.parseAddr("localhost:9301");
    assertEquals("localhost", addr.getHost());
    assertEquals(9301, addr.getPort());

    addr = ElasticBackend.parseAddr("localhost");
    assertEquals("localhost", addr.getHost());
    assertEquals(9300, addr.getPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidPortNumber() throws UnknownHostException {
    InetSocketTransportAddress addr = ElasticBackend.parseAddr("localhost:0x00");
    assertEquals("localhost", addr.getHost());
    assertEquals(0, addr.getPort());
  }
}
