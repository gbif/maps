package org.gbif.maps;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.util.ContextInitializer;
import com.google.common.base.Preconditions;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.gbif.discovery.lifecycle.DiscoveryLifeCycle;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.resource.*;
import org.gbif.occurrence.search.heatmap.es.OccurrenceHeatmapsEsService;

import io.dropwizard.Application;
import io.dropwizard.assets.AssetsBundle;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;

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

    RestHighLevelClient esClient = createEsClient(configuration.getEsConfig());
    OccurrenceHeatmapsEsService heatmapsService =
      new OccurrenceHeatmapsEsService(esClient, configuration.getEsConfig().getIndex());

    // Either use Zookeeper or static config to locate tables
    HBaseMaps hbaseMaps = null;
    if (configuration.getMetastore() != null) {
      MapMetastore meta = Metastores.newZookeeperMapsMeta(configuration.getMetastore().getZookeeperQuorum(), 1000,
                                                  configuration.getMetastore().getPath());
      hbaseMaps = new HBaseMaps(conf, meta, configuration.getHbase().getSaltModulus());

    } else {
      //
      MapMetastore meta = Metastores.newStaticMapsMeta(configuration.getHbase().getTilesTableName(),
                                                       configuration.getHbase().getPointsTableName());
      hbaseMaps = new HBaseMaps(conf, meta, configuration.getHbase().getSaltModulus());
    }


    TileResource tiles = new TileResource(conf,
                                          hbaseMaps,
                                          configuration.getHbase().getTileSize(),
                                          configuration.getHbase().getBufferSize());
    environment.jersey().register(tiles);

    environment.jersey().register(new RegressionResource(tiles, esClient, configuration.getEsConfig().getIndex()));

    // The resource that queries SOLR directly for HeatMap data
    environment.jersey().register(new AddHocMapsResource(heatmapsService,
                                                   configuration.getEsConfig().getTileSize(),
                                                   configuration.getEsConfig().getBufferSize()));

    environment.jersey().register(new BackwardCompatibility(tiles));

    environment.jersey().register(NoContentResponseFilter.class);

    if (configuration.getService().isDiscoverable()) {
      environment.lifecycle().manage(new DiscoveryLifeCycle(configuration.getService()));
    }
  }

  /**
   * Builds an Elasticsearch client using the {@link org.gbif.maps.TileServerConfiguration.EsConfiguration}.
   */
  private static RestHighLevelClient createEsClient(TileServerConfiguration.EsConfiguration esConfig) {
    Objects.requireNonNull(esConfig);
    Objects.requireNonNull(esConfig.getHosts());
    Preconditions.checkArgument(esConfig.getHosts().length > 0);

    HttpHost[] hosts = Arrays.stream(esConfig.getHosts()).map(HttpHost::create).toArray(HttpHost[]::new);
    RestClientBuilder builder =
      RestClient.builder(hosts)
        .setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.setConnectTimeout(esConfig.getConnectTimeout()).setSocketTimeout(esConfig.getSocketTimeout()))
        .setMaxRetryTimeoutMillis(esConfig.getMaxRetryTimeoutMillis());
    return new RestHighLevelClient(builder);
  }
}
