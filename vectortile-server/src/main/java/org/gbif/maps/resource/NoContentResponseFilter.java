package org.gbif.maps.resource;

import java.io.IOException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import org.mortbay.jetty.Response;

/**
 * Sets a 204 for any response that is a NULL object but indicating a 200.
 */
public class NoContentResponseFilter implements ContainerResponseFilter {

  @Override
  public void filter(
    ContainerRequestContext requestContext, ContainerResponseContext responseContext
  ) throws IOException {
    if (responseContext.getStatus() == Response.SC_OK &&
        responseContext.getEntity() != null &&
        responseContext.getEntity() instanceof byte[] &&
        ((byte[])responseContext.getEntity()).length == 0) {
      responseContext.setStatus(Response.SC_NO_CONTENT);
    }
  }
}
