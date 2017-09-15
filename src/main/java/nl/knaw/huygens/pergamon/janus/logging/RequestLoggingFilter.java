package nl.knaw.huygens.pergamon.janus.logging;

import com.google.common.base.Stopwatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.container.PreMatching;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@PreMatching
@Priority(Integer.MIN_VALUE)
public class RequestLoggingFilter implements ContainerRequestFilter, ContainerResponseFilter {
  private static final String MDC_ID = "id";

  private static final String STOPWATCH_PROPERTY = RequestLoggingFilter.class.getName() + "stopwatch";

  private static final Logger LOG = LoggerFactory.getLogger(RequestLoggingFilter.class);

  @Override
  public void filter(ContainerRequestContext context) throws IOException {
    MDC.put(MDC_ID, UUID.randomUUID().toString());
    LOG.info(">     " + context.getMethod() + " " + context.getUriInfo().getRequestUri().toASCIIString());
    context.setProperty(STOPWATCH_PROPERTY, Stopwatch.createStarted());
  }

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
    throws IOException {
    String msg = "< " + responseContext.getStatus() + " " + requestContext.getMethod() + " " +
      requestContext.getUriInfo().getRequestUri().toASCIIString();

    Stopwatch stopwatch = (Stopwatch) requestContext.getProperty(STOPWATCH_PROPERTY);
    if (stopwatch == null) {
      LOG.warn("Lost my stopwatch!");
    } else if (!stopwatch.isRunning()) {
      LOG.warn("Stopwatch was stopped!");
    }
    else {
      msg += String.format(" (%d ms)", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }
    LOG.debug(msg);
  }
}
