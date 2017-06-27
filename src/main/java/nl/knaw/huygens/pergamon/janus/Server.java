package nl.knaw.huygens.pergamon.janus;

import io.dropwizard.Application;
import io.dropwizard.Configuration;
import io.dropwizard.setup.Environment;
import nu.xom.Builder;
import nu.xom.Document;
import nu.xom.ParsingException;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.io.StringReader;
import java.util.function.Function;

/**
 * Web server.
 */
public class Server extends Application<Server.Config> {
  public static class Config extends Configuration {
  }

  @Path("/")
  @Produces(MediaType.APPLICATION_JSON)
  public static class Resource {
    private static Builder parser = new Builder();

    @POST
    public AnnotatedText transform(String input, @QueryParam("offsets") String offsetType)
      throws ParsingException, IOException {
      Function<Document, AnnotatedText> transformer;

      if (offsetType == null) {
        offsetType = "byte";
      }
      switch (offsetType) {
        case "byte":
          transformer = AnnotatedBytes::new;
          break;
        case "utf16":
          transformer = AnnotatedUtf16::new;
          break;
        case "codepoint":
          transformer = AnnotatedCodepoints::new;
          break;
        default:
          throw new WebApplicationException("unknown value for parameter 'offsets'");
      }
      return transformer.apply(parser.build(new StringReader(input)));
    }
  }

  public static void main(String[] args) throws Exception {
    new Server().run(args);
  }

  @Override
  public String getName() {
    return "janus";
  }

  @Override
  public void run(Config configuration, Environment environment) throws Exception {
    environment.jersey().register(new Resource());
  }
}
