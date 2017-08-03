package nl.knaw.huygens.pergamon.janus.graphql;

import com.fasterxml.jackson.annotation.JsonIgnore;
import graphql.ExceptionWhileDataFetching;

class Error extends ExceptionWhileDataFetching {
  Error(ExceptionWhileDataFetching error) {
    super(error.getException());
  }

  @Override
  @JsonIgnore
  public Throwable getException() {
    return super.getException();
  }
}
