package org.gbif.maps;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.util.ContextInitializer;
import org.gbif.common.search.solr.builders.CloudSolrServerBuilder;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.resource.*;
import org.gbif.occurrence.search.heatmap.OccurrenceHeatmapsService;
import org.gbif.ws.discovery.lifecycle.DiscoveryLifeCycle;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.solr.client.solrj.SolrClient;
import org.slf4j.LoggerFactory;

/**
 * The main entry point for running the member node.
 */
public class TileServerApplication extends Application<TileServerConfiguration> {

  private static final String APPLICATION_NAME = "GBIF Tile Server";

  public static void main(String[] args) throws Exception {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    context.reset();
    ContextInitializer initializer = new ContextInitializer(context);
    initializer.autoConfig();

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
  public final void run(TileServerConfiguration configuration, Environment environment) throws Exception {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", configuration.getHbase().getZookeeperQuorum());

    SolrClient client = new CloudSolrServerBuilder()
      .withZkHost(configuration.getSolr().getZookeeperQuorum())
      .withDefaultCollection(configuration.getSolr().getDefaultCollection()).build();
    OccurrenceHeatmapsService solrService = new OccurrenceHeatmapsService(client,
                                                                          configuration.getSolr().getRequestHandler());


    // Either use Zookeeper or static config to locate tables
    HBaseMaps hbaseMaps = null;
    if (configuration.getMetastore() != null) {
      MapMetastore meta = Metastores.newZookeeperMapsMeta(configuration.getMetastore().getZookeeperQuorum(), 1000,
                                                  configuration.getMetastore().getPath());
      hbaseMaps = new HBaseMaps(conf, meta, configuration.getHbase().getSaltModulus());

    } else {
      //
      MapMetastore meta = Metastores.newStaticMapsMeta(configuration.getHbase().getTableName(),
                                                       configuration.getHbase().getTableName());
      hbaseMaps = new HBaseMaps(conf, meta, configuration.getHbase().getSaltModulus());
    }


    TileResource tiles = new TileResource(conf,
                                          hbaseMaps,
                                          configuration.getHbase().getTileSize(),
                                          configuration.getHbase().getBufferSize());
    environment.jersey().register(tiles);

    environment.jersey().register(new RegressionResource(tiles, client));

    // The resource that queries SOLR directly for HeatMap data
    environment.jersey().register(new SolrResource(solrService,
                                                   configuration.getSolr().getTileSize(),
                                                   configuration.getSolr().getBufferSize()));

    environment.jersey().register(new BackwardCompatibility(tiles));

    environment.jersey().register(NoContentResponseFilter.class);

    if (configuration.getService().isDiscoverable()) {
      environment.lifecycle().manage(new DiscoveryLifeCycle(configuration.getService()));
    }
  }
}
