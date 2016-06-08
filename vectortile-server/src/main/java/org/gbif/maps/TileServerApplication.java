package org.gbif.maps;

import org.gbif.maps.resource.DensityResource;
import org.gbif.maps.resource.HexDensityResource;
import org.gbif.maps.resource.PointResource;
import org.gbif.maps.resource.SolrResource;
import org.gbif.maps.resource.TileResource;
import org.gbif.maps.resource.TileResourceOrig;

import java.io.IOException;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.client.HttpClientBuilder;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.http.client.HttpClient;

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
    environment.jersey().register(new TileResourceOrig());
    environment.jersey().register(new DensityResource());
    environment.jersey().register(new HexDensityResource());
    environment.jersey().register(new PointResource());




    final HttpClient httpClient = new HttpClientBuilder(environment).using(configuration.getHttpClientConfiguration())
                                                                    .build("example-http-client");

    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "c1n2.gbif.org:2181,c1n3.gbif.org:2181,c1n1.gbif.org:2181");
    conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    environment.jersey().register(new SolrResource(conf, 512, 25, httpClient));

    environment.jersey().register(new TileResource(conf, 4096, 25));

  }
}
