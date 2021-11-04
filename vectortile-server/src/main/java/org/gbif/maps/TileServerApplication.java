package org.gbif.maps;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpHost;
import org.cache2k.Cache;
import org.cache2k.config.Cache2kConfig;
import org.cache2k.extra.spring.SpringCache2kCacheManager;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import org.gbif.api.model.occurrence.predicate.Predicate;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.resource.*;
import org.gbif.occurrence.search.cache.DefaultInMemoryPredicateCacheService;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.search.heatmap.es.OccurrenceHeatmapsEsService;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.mybatis.spring.boot.autoconfigure.MybatisAutoConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * The main entry point for running the member node.
 */
@SpringBootApplication(
  scanBasePackages = { "org.gbif.maps"},
  exclude = {
    RabbitAutoConfiguration.class,
    MybatisAutoConfiguration.class
  })
@EnableConfigurationProperties
@EnableCaching
public class TileServerApplication  {

  public static void main(String[] args) {
    SpringApplication.run(TileServerApplication.class, args);
  }

  /**
   * This class is required to redirect the "debug" sub context to the default home page.
   */
  @org.springframework.context.annotation.Configuration
  public static class WebServerStaticResourceConfiguration implements WebMvcConfigurer {
    @Override
    public void addViewControllers(ViewControllerRegistry registry) {
      registry.addViewController("/debug/").setViewName("forward:/debug/index.html");
      registry.addViewController("/debug/ol/").setViewName("forward:/debug/ol/index.html");
      registry.addViewController("/debug/comparison/").setViewName("forward:/debug/comparison/index.html");
    }

  }

  @org.springframework.context.annotation.Configuration
  public static class TileServerSpringConfiguration {

    @ConfigurationProperties
    @Bean
    TileServerConfiguration tileServerConfiguration() {
      return new TileServerConfiguration();
    }

    @Bean
    public RestHighLevelClient provideEsClient(TileServerConfiguration tileServerConfiguration) {
      EsConfig esConfig = tileServerConfiguration.getEsConfiguration().getElasticsearch();

      HttpHost[] hosts = new HttpHost[esConfig.getHosts().length];
      int i = 0;
      for (String host : esConfig.getHosts()) {
        try {
          URL url = new URL(host);
          hosts[i] = new HttpHost(url.getHost(), url.getPort(), url.getProtocol());
          i++;
        } catch (MalformedURLException e) {
          throw new IllegalArgumentException(e.getMessage(), e);
        }
      }

      SniffOnFailureListener sniffOnFailureListener =
        new SniffOnFailureListener();

      RestClientBuilder builder =
        RestClient.builder(hosts)
          .setRequestConfigCallback(
            requestConfigBuilder ->
              requestConfigBuilder
                .setConnectTimeout(esConfig.getConnectTimeout())
                .setSocketTimeout(esConfig.getSocketTimeout()))
          .setNodeSelector(NodeSelector.SKIP_DEDICATED_MASTERS);

      if (esConfig.getSniffInterval() > 0) {
        builder.setFailureListener(sniffOnFailureListener);
      }

      RestHighLevelClient highLevelClient = new RestHighLevelClient(builder);

      if (esConfig.getSniffInterval() > 0) {
        Sniffer sniffer = Sniffer.builder(highLevelClient.getLowLevelClient())
          .setSniffIntervalMillis(esConfig.getSniffInterval())
          .setSniffAfterFailureDelayMillis(esConfig.getSniffAfterFailureDelay())
          .build();
        sniffOnFailureListener.setSniffer(sniffer);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          sniffer.close();
          try {
            highLevelClient.close();
          } catch (IOException e) {
            throw new IllegalStateException("Couldn't close ES client", e);
          }
        }));
      }

      return highLevelClient;
    }

    @Bean
    OccurrenceHeatmapsEsService occurrenceHeatmapsEsService(RestHighLevelClient esClient, TileServerConfiguration tileServerConfiguration) {
      return new OccurrenceHeatmapsEsService(esClient, tileServerConfiguration.getEsConfiguration().getElasticsearch().getIndex());
    }

    @Bean
    HBaseMaps hBaseMaps(TileServerConfiguration tileServerConfiguration) throws Exception {
      // Either use Zookeeper or static config to locate tables
      Configuration conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum", tileServerConfiguration.getHbase().getZookeeperQuorum());

      if (tileServerConfiguration.getMetastore() != null) {
        MapMetastore meta = Metastores.newZookeeperMapsMeta(tileServerConfiguration.getMetastore().getZookeeperQuorum(), 1000,
                                                            tileServerConfiguration.getMetastore().getPath());
        return new HBaseMaps(conf, meta, tileServerConfiguration.getHbase().getSaltModulus());

      } else {
        //
        MapMetastore meta = Metastores.newStaticMapsMeta(tileServerConfiguration.getHbase().getTilesTableName(),
                                                         tileServerConfiguration.getHbase().getPointsTableName());
        return new HBaseMaps(conf, meta, tileServerConfiguration.getHbase().getSaltModulus());
      }
    }

    @Primary
    @Bean
    public ObjectMapper registryObjectMapper() {
      return JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport();
    }

    @ConfigurationProperties(prefix = "cache.predicates")
    @Bean
    public Cache2kConfig<Integer,Predicate> predicateCache2kConfig() {
      return new Cache2kConfig<>();
    }

    @Bean
    public PredicateCacheService predicateCacheService(ObjectMapper objectMapper, Cache2kConfig<Integer,Predicate> cache2kConfig) {
      return new DefaultInMemoryPredicateCacheService(objectMapper, cache2kConfig.builder().build());
    }

  }
}
