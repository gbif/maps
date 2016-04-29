package org.gbif.maps;

import org.gbif.maps.resource.TileResource;

import java.io.IOException;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.commons.io.IOExceptionWithCause;

/**
 * The main entry point for running the member node.
 */
public class TileServerApplication extends Application<TileServerConfiguration> {

  private static final String APPLICATION_NAME = "GBIF Tile Server";

  public static void main(String[] args) throws Exception {
    new TileServerApplication().run(args);
  }

  @Override
  public String getName() {
    return APPLICATION_NAME;
  }

  @Override
  public final void initialize(Bootstrap<TileServerConfiguration> bootstrap) {
    bootstrap.addBundle(new AssetsBundle("/assets", "/", "index.html", "assets"));
  }

  @Override
  public final void run(TileServerConfiguration configuration, Environment environment) throws IOException {
    environment.jersey().register(new TileResource());
    //environment.jersey().setUrlPattern("/api/*");

  }
}
