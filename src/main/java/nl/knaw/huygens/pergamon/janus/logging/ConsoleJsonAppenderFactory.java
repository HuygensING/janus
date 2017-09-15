package nl.knaw.huygens.pergamon.janus.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.ConsoleAppender;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.logging.AbstractAppenderFactory;
import io.dropwizard.logging.async.AsyncAppenderFactory;
import io.dropwizard.logging.filter.LevelFilterFactory;
import io.dropwizard.logging.layout.LayoutFactory;
import net.logstash.logback.encoder.LogstashEncoder;

import java.util.HashMap;

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
      AppenderFactoryHelper.getCustomFieldsFromHashMap(customFields).ifPresent(encoder::setCustomFields);
    }

    // allow overriding default field names, e.g.,
    //   fieldNames:
    //     "message": "msg"
    //     "timestamp": "@time"
    if (fieldNames != null) {
      encoder.setFieldNames(AppenderFactoryHelper.getFieldNamesFromHashMap(fieldNames));
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

}
