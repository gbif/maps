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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Optional;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.http.HttpHost;
import org.cache2k.config.Cache2kConfig;
import org.cache2k.extra.spring.SpringCache2kCacheManager;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.gbif.api.model.Constants;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.event.search.es.EventEsField;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.io.PointFeature;
import org.gbif.maps.resource.*;
import org.gbif.occurrence.common.json.OccurrenceSearchParameterMixin;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.occurrence.search.es.OccurrenceBaseEsFieldMapper;
import org.gbif.occurrence.search.es.OccurrenceEsField;
import org.gbif.occurrence.search.heatmap.es.OccurrenceHeatmapsEsService;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.vocabulary.client.ConceptClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.gbif.ws.server.processor.ParamNameProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
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
import org.springframework.http.converter.json.AbstractJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.validation.constraints.NotNull;

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

    OccurrenceHeatmapsEsService heatmapsEsService(RestHighLevelClient esClient,
                                                  TileServerConfiguration.EsTileConfiguration esTileConfiguration,
                                                  ConceptClient conceptClient,
                                                  NameUsageMatchingService nameUsageMatchingService,
                                                  String defaultChecklistKey) {
      return new OccurrenceHeatmapsEsService(esClient,
        esTileConfiguration.getElasticsearch().getIndex(),
        esFieldMapper(esTileConfiguration, defaultChecklistKey),
        conceptClient, nameUsageMatchingService);
    }

    /**
     * Custom {@link BeanPostProcessor} for adding {@link ParamNameProcessor}.
     *
     * @return BeanPostProcessor
     */
    @Bean
    public BeanPostProcessor beanPostProcessor() {

      return new BeanPostProcessor() {

        @Override
        public Object postProcessBeforeInitialization(@NotNull Object bean, String beanName) {
          return bean;
        }

        @Override
        public Object postProcessAfterInitialization(@NotNull Object bean, String beanName) {
          if (bean instanceof AbstractJackson2HttpMessageConverter) {
            AbstractJackson2HttpMessageConverter converter = (AbstractJackson2HttpMessageConverter) bean;
            ObjectMapper objectMapper = converter.getObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());

            objectMapper.registerModule(new com.fasterxml.jackson.databind.module.SimpleModule()
              .addKeyDeserializer(OccurrenceSearchParameter.class,
                new OccurrenceSearchParameter.OccurrenceSearchParameterKeyDeserializer()
              )
              .addDeserializer(OccurrenceSearchParameter.class,
                new OccurrenceSearchParameter.OccurrenceSearchParameterDeserializer()
              )
            );
            objectMapper.addMixIn(SearchParameter.class, OccurrenceSearchParameterMixin.class);
          }
          return bean;
        }
      };
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

    OccurrenceBaseEsFieldMapper esFieldMapper(TileServerConfiguration.EsTileConfiguration esTileConfiguration,
                                              String defaultChecklistKey) {
      if (TileServerConfiguration.EsTileConfiguration.SearchType.EVENT == esTileConfiguration.getType() ){
        return EventEsField.buildFieldMapper();
      }
      return OccurrenceEsField.buildFieldMapper(defaultChecklistKey);
    }

    @Bean("occurrenceHeatmapsEsService")
    @ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
    OccurrenceHeatmapsEsService occurrenceHeatmapsEsService(
        @Qualifier("esOccurrenceClient") RestHighLevelClient esClient,
        TileServerConfiguration tileServerConfiguration,
        ConceptClient conceptClient,
        NameUsageMatchingService nameUsageMatchingService,
        @Value("${defaultChecklistKey: 'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c'}") String defaultChecklistKey) {
      return heatmapsEsService(
          esClient, tileServerConfiguration.getEsOccurrenceConfiguration(), conceptClient, nameUsageMatchingService, defaultChecklistKey);
    }

    @Bean("eventHeatmapsEsService")
    @ConditionalOnExpression("${esEventConfiguration.enabled}")
    OccurrenceHeatmapsEsService eventHeatmapsEsService(
        @Qualifier("esEventClient") RestHighLevelClient esClient,
        TileServerConfiguration tileServerConfiguration,
        ConceptClient conceptClient,
        NameUsageMatchingService nameUsageMatchingService,
        @Value("${defaultChecklistKey: 'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c'}") String defaultChecklistKey) {
      return heatmapsEsService(
          esClient, tileServerConfiguration.getEsEventConfiguration(), conceptClient, nameUsageMatchingService, defaultChecklistKey);
    }

    @Bean
    @Profile("!es-only")
    HBaseMaps hBaseMaps(TileServerConfiguration tileServerConfiguration, SpringCache2kCacheManager cacheManager, MeterRegistry meterRegistry,
                        Cache2kConfig<String, Optional<PointFeature.PointFeatures>> pointCacheConfiguration,
                        Cache2kConfig<HBaseMaps.TileKey, Optional<byte[]>> tileCacheConfiguration) throws Exception {
      // Either use Zookeeper or static config to locate tables
      Configuration conf = HBaseConfiguration.create();
      conf.set("hbase.zookeeper.quorum", tileServerConfiguration.getHbase().getZookeeperQuorum());
      String hbaseZNode = tileServerConfiguration.getHbase().getHbaseZnode();
      if (!Strings.isNullOrEmpty(hbaseZNode)) {
        conf.set("zookeeper.znode.parent", hbaseZNode);
      }

      if (tileServerConfiguration.getMetastore() != null) {
        MapMetastore meta = Metastores.newZookeeperMapsMeta(tileServerConfiguration.getMetastore().getZookeeperQuorum(), 1000,
          tileServerConfiguration.getMetastore().getPath());
        return new HBaseMaps(conf, meta, tileServerConfiguration.getHbase().getSaltModulus(), cacheManager, meterRegistry, pointCacheConfiguration, tileCacheConfiguration);

      } else {
        MapMetastore meta = Metastores.newStaticMapsMeta(tileServerConfiguration.getHbase().getTilesTableName(),
          tileServerConfiguration.getHbase().getPointsTableName());
        return new HBaseMaps(conf, meta, tileServerConfiguration.getHbase().getSaltModulus(), cacheManager, meterRegistry, pointCacheConfiguration, tileCacheConfiguration);
      }
    }

    @Primary
    @Bean
    public ObjectMapper objectMapper() {
      return JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport()
                                            .addMixIn(SearchParameter.class, OccurrenceSearchParameterMixin.class)
                                            .registerModule(new JavaTimeModule());
    }

    @Bean
    @ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
    public ConceptClient conceptClient(@Value("${api.url}") String apiUrl) {
      return new ClientBuilder()
        .withObjectMapper(
          JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport()
            .registerModule(new JavaTimeModule()))
        .withUrl(apiUrl)
        .build(ConceptClient.class);
    }

    @Bean
    public NameUsageMatchingService nameUsageMatchingService(@Value("${api.url}") String apiUrl) {
      if (apiUrl.endsWith("/v1/")) {
        // remove the version from the URL
        apiUrl = apiUrl.substring(0, apiUrl.length() - 3);
      }
      return new ClientBuilder()
        .withUrl(apiUrl)
        .withObjectMapper(JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport())
        .withFormEncoder()
        .withExponentialBackoffRetry(Duration.ofMillis(250), 1.0, 3)
        .build(NameUsageMatchingService.class);
    }
  }
}
