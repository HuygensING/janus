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
import javax.ws.rs.core.MultivaluedMap;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@PreMatching
@Priority(Integer.MIN_VALUE)
public class RequestLoggingFilter implements ContainerRequestFilter, ContainerResponseFilter {
  private static final Comparator<Map.Entry<String, List<String>>> BY_KEY_IGNORING_CASE =
    (e1, e2) -> e1.getKey().compareToIgnoreCase(e2.getKey());

  private static final String STOPWATCH_PROPERTY = RequestLoggingFilter.class.getName() + "stopwatch";

  private static final Logger LOG = LoggerFactory.getLogger(RequestLoggingFilter.class);

  private static final String MDC_COMMIT_HASH = "commit_hash";
  private static final String MDC_ELAPSED_MS = "elapsed_ms";
  private static final String MDC_ID = "id";
  private static final String MDC_LOG_TYPE = "type";
  private static final String MDC_REQUEST_AUTHORITY = "request_authority";
  private static final String MDC_REQUEST_HEADERS = "request_headers";
  private static final String MDC_REQUEST_METHOD = "http_method";
  private static final String MDC_REQUEST_PATH = "request_path";
  private static final String MDC_REQUEST_QUERY = "request_query";
  private static final String MDC_REQUEST_URI = "request_uri";
  private static final String MDC_RESPONSE_HEADERS = "response_headers";
  private static final String MDC_RESPONSE_STATUS = "response_status";

  private static final List<String> MDC_REQUEST_SPECIFIC = Arrays.asList(
    MDC_ID, MDC_ELAPSED_MS, MDC_LOG_TYPE,
    MDC_REQUEST_AUTHORITY, MDC_REQUEST_HEADERS, MDC_REQUEST_METHOD, MDC_REQUEST_PATH, MDC_REQUEST_QUERY, MDC_REQUEST_URI,
    MDC_RESPONSE_HEADERS, MDC_RESPONSE_STATUS
  );

  private final String commitHash;

  public RequestLoggingFilter(String commitHash) {
    this.commitHash = commitHash;
  }

  @Override
  public void filter(ContainerRequestContext context) throws IOException {
    MDC.put(MDC_COMMIT_HASH, commitHash);
    MDC.put(MDC_ID, UUID.randomUUID().toString());
    MDC.put(MDC_LOG_TYPE, "request");
    MDC.put(MDC_REQUEST_METHOD, context.getMethod());

    final URI requestUri = context.getUriInfo().getRequestUri();
    MDC.put(MDC_REQUEST_URI, requestUri.toASCIIString());
    MDC.put(MDC_REQUEST_PATH, requestUri.getPath());
    MDC.put(MDC_REQUEST_AUTHORITY, requestUri.getAuthority());
    MDC.put(MDC_REQUEST_QUERY, requestUri.getQuery());
    MDC.put(MDC_REQUEST_HEADERS, formatHeaders(context.getHeaders()));

    LOG.info(">     " + context.getMethod() + " " + requestUri.toASCIIString());

    context.setProperty(STOPWATCH_PROPERTY, Stopwatch.createStarted());
  }

  @Override
  public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext)
    throws IOException {
    MDC.put(MDC_LOG_TYPE, "response");
    MDC.put(MDC_RESPONSE_STATUS, String.valueOf(responseContext.getStatus()));

    String msg = "< " + responseContext.getStatus() + " " + requestContext.getMethod() + " " +
      requestContext.getUriInfo().getRequestUri().toASCIIString();

    Stopwatch stopwatch = (Stopwatch) requestContext.getProperty(STOPWATCH_PROPERTY);
    if (stopwatch == null) {
      LOG.warn("Lost my stopwatch!");
    } else if (!stopwatch.isRunning()) {
      LOG.warn("Stopwatch was stopped!");
    } else {
      MDC.put(MDC_ELAPSED_MS, String.valueOf(stopwatch.elapsed(TimeUnit.MILLISECONDS)));
      msg += String.format(" (%d ms)", stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    MDC.put(MDC_RESPONSE_HEADERS, formatHeaders(responseContext.getStringHeaders()));

    LOG.debug(msg);

    clearMdc();
  }

  private void clearMdc() {
    MDC_REQUEST_SPECIFIC.forEach(MDC::remove); // remove only the request specific stuff
  }

  private String formatHeaders(final MultivaluedMap<String, String> headers) {
    final StringBuilder builder = new StringBuilder();
    sortHeaders(headers.entrySet())
      .forEach(entry -> builder.append(entry.getKey())
                               .append(": ")
                               .append(String.join(",", entry.getValue()))
                               .append('\n'));
    return builder.toString();
  }

  private Set<Map.Entry<String, List<String>>> sortHeaders(final Set<Map.Entry<String, List<String>>> headers) {
    final TreeSet<Map.Entry<String, List<String>>> sorted = new TreeSet<>(BY_KEY_IGNORING_CASE);
    sorted.addAll(headers);
    return sorted;
  }

}
