package nl.knaw.huygens.pergamon.janus.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.dropwizard.logging.AbstractAppenderFactory;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;
import net.logstash.logback.encoder.LogstashEncoder;
import net.logstash.logback.fieldnames.LogstashFieldNames;

import java.util.HashMap;
import java.util.Optional;

@JsonTypeName("console-json")
public class ConsoleJsonAppenderFactory extends AbstractAppenderFactory<ILoggingEvent> {
  @JsonProperty
  protected HashMap<String, String> customFields;

  @JsonProperty
  protected HashMap<String, String> fieldNames;

  @JsonProperty
  protected boolean includeMdc = true;

  @JsonProperty
  protected boolean includeContext = false;

  @JsonProperty
  protected boolean includeCallerData = false;

  @Override
  public Appender<ILoggingEvent> build(LoggerContext context, String applicationName,
                                       LayoutFactory<ILoggingEvent> layoutFactory,
                                       LevelFilterFactory<ILoggingEvent> levelFilterFactory,
                                       AsyncAppenderFactory<ILoggingEvent> asyncAppenderFactory) {
    final ConsoleAppender<ILoggingEvent> appender = new ConsoleAppender<>();
    final LogstashEncoder encoder = new LogstashEncoder();

    appender.setName("console-json-appender");
    appender.setContext(context);

    // add logback context
    encoder.setIncludeContext(includeContext);

    // add Mapped Diagnostic Context from org.slf4j.MDC;
    encoder.setIncludeMdc(includeMdc);

    // add caller data if present in event (expensive)
    encoder.setIncludeCallerData(includeCallerData);

    // add any yaml-supplied custom fields, e.g.,
    //   customFields:
    //     "appName": "Janus"
    if (customFields != null) {
      getCustomFieldsFromHashMap(customFields).ifPresent(encoder::setCustomFields);
    }

    // allow overriding default field names, e.g.,
    //   fieldNames:
    //     "message": "msg"
    //     "timestamp": "@time"
    if (fieldNames != null) {
      encoder.setFieldNames(getFieldNamesFromHashMap(fieldNames));
    }

    // setup encoder
    appender.setEncoder(encoder);

    // setup filters
    appender.addFilter(levelFilterFactory.build(threshold));
    getFilterFactories().forEach(f -> appender.addFilter(f.build()));

    // let's go
    encoder.start();
    appender.start();

    return wrapAsync(appender, asyncAppenderFactory);
  }

  private static Optional<String> getCustomFieldsFromHashMap(HashMap<String, String> map) {
    try {
      return Optional.of(new ObjectMapper().writeValueAsString(map));
    } catch (JsonProcessingException e) {
      System.err.println("Unable to parse customFields: " + e.getMessage());
      return Optional.empty();
    }
  }

  private static LogstashFieldNames getFieldNamesFromHashMap(HashMap<String, String> map) {
    LogstashFieldNames fieldNames = new LogstashFieldNames();

    fieldNames.setTimestamp(map.getOrDefault("timestamp", "@timestamp"));
    fieldNames.setVersion(map.getOrDefault("version", "@version"));
    fieldNames.setMessage(map.getOrDefault("message", "message"));
    fieldNames.setLogger(map.getOrDefault("logger", "logger_name"));
    fieldNames.setThread(map.getOrDefault("thread", "thread_name"));
    fieldNames.setLevel(map.getOrDefault("level", "level"));
    fieldNames.setLevelValue(map.getOrDefault("levelValue", "level_value"));
    fieldNames.setCaller(map.get("caller"));
    fieldNames.setCallerClass(map.getOrDefault("callerClass", "caller_class_name"));
    fieldNames.setCallerMethod(map.getOrDefault("callerMethod", "caller_method_name"));
    fieldNames.setCallerFile(map.getOrDefault("callerFile", "caller_file_name"));
    fieldNames.setCallerLine(map.getOrDefault("callerLine", "caller_line_number"));
    fieldNames.setStackTrace(map.getOrDefault("stackTrace", "stack_trace"));
    fieldNames.setTags(map.getOrDefault("tags", "tags"));
    fieldNames.setMdc(map.get("mdc"));
    fieldNames.setContext(map.get("context"));

    return fieldNames;
  }
}
