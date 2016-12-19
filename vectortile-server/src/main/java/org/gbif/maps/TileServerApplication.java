package org.gbif.maps;

import org.gbif.common.search.solr.builders.CloudSolrServerBuilder;
import org.gbif.maps.resource.NoContentResponseFilter;
import org.gbif.maps.resource.RegressionResource;
import org.gbif.maps.resource.SolrResource;
import org.gbif.maps.resource.TileResource;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapsService;
import org.gbif.ws.discovery.lifecycle.DiscoveryLifeCycle;

import java.io.IOException;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.solr.client.solrj.SolrClient;

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
    // We expect the assets bundle to be mounted on / in the config (applicationContextPath: "/")
    // Here we intercept the /map/debug/* URLs and serve up the content from /assets folder instead
    bootstrap.addBundle(new AssetsBundle("/assets", "/map/debug", "index.html", "assets"));
  }

  @Override
  public final void run(TileServerConfiguration configuration, Environment environment) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", configuration.getHbase().getZookeeperQuorum());

    SolrClient client = new CloudSolrServerBuilder()
      .withZkHost(configuration.getSolr().getZookeeperQuorum())
      .withDefaultCollection(configuration.getSolr().getDefaultCollection()).build();
    OccurrenceHeatmapsService solrService = new OccurrenceHeatmapsService(client,
                                                                          configuration.getSolr().getRequestHandler());


    // tileSize must match the preprocessed tiles in HBase
    String tableName = configuration.getHbase().getTableName();

    TileResource tiles = new TileResource(conf,
                                          configuration.getHbase().getTableName(),
                                          configuration.getHbase().getTileSize(),
                                          configuration.getHbase().getBufferSize(),
                                          configuration.getHbase().getSaltModulus());
    environment.jersey().register(tiles);

    environment.jersey().register(new RegressionResource(tiles, client));

    // The resource that queries SOLR directly for HeatMap data
    environment.jersey().register(new SolrResource(solrService,
                                                   configuration.getSolr().getTileSize(),
                                                   configuration.getSolr().getBufferSize()));

    environment.jersey().register(NoContentResponseFilter.class);

    if (configuration.getService().isDiscoverable()) {
      environment.lifecycle().manage(new DiscoveryLifeCycle(configuration.getService()));
    }
  }
}
