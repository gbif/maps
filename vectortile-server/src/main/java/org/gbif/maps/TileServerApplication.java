package org.gbif.maps;

import org.gbif.common.search.solr.builders.CloudSolrServerBuilder;
import org.gbif.maps.resource.SolrResource;
import org.gbif.maps.resource.TileResource;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapsService;

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
    bootstrap.addBundle(new AssetsBundle("/assets", "/", "index.html", "assets"));
  }

  @Override
  public final void run(TileServerConfiguration configuration, Environment environment) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    // TODO: configurify!

    conf.set("hbase.zookeeper.quorum", "c1n2.gbif.org:2181,c1n3.gbif.org:2181,c1n1.gbif.org:2181");
    //conf.set("hbase.zookeeper.quorum", "prodmaster1-vh.gbif.org:2181,prodmaster2-vh.gbif.org:2181,prodmaster3-vh.gbif.org:2181");
    conf.setInt("hbase.zookeeper.property.clientPort", 2181);
    SolrClient client = new CloudSolrServerBuilder()
      .withZkHost("prodmaster1-vh.gbif.org:2181,prodmaster2-vh.gbif.org:2181,prodmaster3-vh.gbif.org:2181/prodsolr")
      .withDefaultCollection("occurrence_b").build();
    OccurrenceHeatmapsService solrService = new OccurrenceHeatmapsService(client, "occurrence");

    environment.jersey().register(new SolrResource(conf, 512, 25, solrService));

    // tileSize must match the preprocessed tiles in HBase
    String tableName = "tim_test";

    // 3000 maps are tile pyramided

    //environment.jersey().register(new TileResource(conf, tableName, 4096, 25));  // Too big @ 1MB
    //environment.jersey().register(new TileResource(conf, tableName, 1024, 25));  // Too slow still
    //environment.jersey().register(new TileResource(conf, tableName, 512, 25)); // 1.5MB unc., 500KB comp.
    environment.jersey().register(new TileResource(conf, tableName, 512, 64)); // 1.5MB unc., 500KB comp.
    //environment.jersey().register(new TileResource(conf, tableName, 256, 25));  // Too ugly
    environment.jersey().register(new SolrResource(conf, 512, 64, solrService));
  }
}
