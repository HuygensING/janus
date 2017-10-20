package nl.knaw.huygens.pergamon.janus.graphql;

import com.coxautodev.graphql.tools.SchemaParser;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.schema.GraphQLSchema;
import io.swagger.annotations.Api;
import nl.knaw.huygens.pergamon.janus.ElasticBackend;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import static graphql.GraphQL.newGraphQL;

@Api(GraphQLResource.PATH)
@Path(GraphQLResource.PATH)
@Consumes("application/graphql")
@Produces(MediaType.APPLICATION_JSON)
public class GraphQLResource {
  static final String PATH = "graphql";

  private final ElasticBackend backend;
  private final GraphQLSchema schema;

  public GraphQLResource(ElasticBackend backend) {
    this.backend = backend;
    schema = SchemaParser.newParser()
                         .file("schema.graphqls")
                         .resolvers(new Query(backend))
                         .build()
                         .makeExecutableSchema();
  }

  @POST
  public Object run(String query) {
    GraphQL graphql = newGraphQL(schema).build();
    ExecutionResult result = graphql.execute(query, backend);
    return new Response(result);
  }
}
