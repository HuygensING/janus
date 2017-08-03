package nl.knaw.huygens.pergamon.janus.graphql;

import graphql.ExceptionWhileDataFetching;
import graphql.ExecutionResult;
import graphql.GraphQLError;

import java.util.List;
import java.util.stream.Collectors;

class Response {
  private final Object data;
  private final List<GraphQLError> errors;

  Response(ExecutionResult result) {
    this.data = result.getData();
    this.errors = result
      .getErrors()
      .stream()
      .map(e -> e instanceof ExceptionWhileDataFetching ? new Error((ExceptionWhileDataFetching) e) : e)
      .collect(Collectors.toList());
  }

  public Object getData() {
    return data;
  }

  public List<GraphQLError> getErrors() {
    return errors;
  }
}
