/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gbif.maps;

import org.gbif.api.model.predicate.Predicate;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.resource.*;
import org.gbif.occurrence.search.cache.DefaultInMemoryPredicateCacheService;
import org.gbif.occurrence.search.cache.PredicateCacheService;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.event.search.es.EventEsField;
import org.gbif.occurrence.search.heatmap.es.OccurrenceHeatmapsEsService;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.http.HttpHost;
import org.cache2k.config.Cache2kConfig;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.autoconfigure.elasticsearch.ElasticSearchRestHealthContributorAutoConfiguration;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * The main entry point for running the member node.
 */
@SpringBootApplication(
  scanBasePackages = {
    "org.gbif.maps",
    "org.gbif.ws.server.mapper"
  },
  exclude = {
    RabbitAutoConfiguration.class,
    ElasticSearchRestHealthContributorAutoConfiguration.class
  })
@EnableConfigurationProperties
public class TileServerApplication {

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

    @Bean("esOccurrenceClient")
    @ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
    public RestHighLevelClient provideOccurrenceEsClient(TileServerConfiguration tileServerConfiguration) {
      return provideEsClient(tileServerConfiguration.getEsOccurrenceConfiguration().getElasticsearch());
    }

    @Bean("esEventClient")
    @ConditionalOnExpression("${esEventConfiguration.enabled}")
    public RestHighLevelClient provideEventEsClient(TileServerConfiguration tileServerConfiguration) {
      return provideEsClient(tileServerConfiguration.getEsEventConfiguration().getElasticsearch());
    }

    private RestHighLevelClient provideEsClient(EsConfig esConfig) {
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

    OccurrenceHeatmapsEsService heatmapsEsService(RestHighLevelClient esClient, TileServerConfiguration.EsTileConfiguration esTileConfiguration) {
      return new OccurrenceHeatmapsEsService(esClient,
                                             esTileConfiguration.getElasticsearch().getIndex(),
                                             esFieldMapper(esTileConfiguration));
    }

    @Bean
    OccurrenceBaseEsFieldMapper esFieldMapper(TileServerConfiguration.EsTileConfiguration esTileConfiguration) {
      if (TileServerConfiguration.EsTileConfiguration.SearchType.EVENT == esTileConfiguration.getType() ){
        return EventEsField.buildFieldMapper();
      }
      return OccurrenceEsField.buildFieldMapper();
    }

    @Bean("occurrenceHeatmapsEsService")
    @ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
    OccurrenceHeatmapsEsService occurrenceHeatmapsEsService(@Qualifier("esOccurrenceClient") RestHighLevelClient esClient, TileServerConfiguration tileServerConfiguration) {
      return heatmapsEsService(esClient, tileServerConfiguration.getEsOccurrenceConfiguration());
    }

    @Bean("eventHeatmapsEsService")
    @ConditionalOnExpression("${esEventConfiguration.enabled}")
    OccurrenceHeatmapsEsService eventHeatmapsEsService(@Qualifier("esEventClient") RestHighLevelClient esClient, TileServerConfiguration tileServerConfiguration) {
      return heatmapsEsService(esClient, tileServerConfiguration.getEsEventConfiguration());
    }

    @Bean
    @Profile("!es-only")
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

    @Bean("occurrencePredicateCache")
    @ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
    public PredicateCacheService occurrencePredicateCacheService(ObjectMapper objectMapper, Cache2kConfig<Integer, Predicate> cache2kConfig) {
      return new DefaultInMemoryPredicateCacheService(objectMapper, cache2kConfig.builder().name("occurrencePredicateCache").build());
    }

    @Bean("eventPredicateCache")
    @ConditionalOnExpression("${esEventConfiguration.enabled}")
    public PredicateCacheService eventPredicateCacheService(ObjectMapper objectMapper, Cache2kConfig<Integer, Predicate> cache2kConfig) {
      return new DefaultInMemoryPredicateCacheService(objectMapper, cache2kConfig.builder().name("eventPredicateCache").build());
    }

  }
}
