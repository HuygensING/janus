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

    // IPv6
    addr = ElasticBackend.parseAddr("[::1]:9300");
    assertEquals("0:0:0:0:0:0:0:1", addr.getHost());
    assertEquals(9300, addr.getPort());

    addr = ElasticBackend.parseAddr("[::1]");
    assertEquals("0:0:0:0:0:0:0:1", addr.getHost());
    assertEquals(9300, addr.getPort());
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidIPv6() throws UnknownHostException {
    InetSocketTransportAddress addr = ElasticBackend.parseAddr("[::1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void invalidPortNumber() throws UnknownHostException {
    InetSocketTransportAddress addr = ElasticBackend.parseAddr("localhost:0x00");
  }
}
