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
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Optional;
import javax.validation.constraints.NotNull;
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
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.maps.common.meta.MapMetastore;
import org.gbif.maps.common.meta.Metastores;
import org.gbif.maps.resource.HBaseMaps;
import org.gbif.maps.io.PointFeature;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.es.occurrence.OccurrenceEsField;
import org.gbif.search.heatmap.es.occurrence.OccurrenceEsHeatmapRequestBuilder;
import org.gbif.search.heatmap.es.occurrence.OccurrenceHeatmapsEsService;
import org.gbif.vocabulary.client.ConceptClient;
import org.gbif.ws.client.ClientBuilder;
import org.gbif.ws.json.JacksonJsonObjectMapperProvider;
import org.gbif.ws.server.processor.ParamNameProcessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.boot.autoconfigure.elasticsearch.ElasticsearchRestClientAutoConfiguration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.http.converter.json.AbstractJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.PathMatchConfigurer;
import org.springframework.web.servlet.config.annotation.ViewControllerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

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
    ElasticsearchRestClientAutoConfiguration.class
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

    @Override
    public void configurePathMatch(PathMatchConfigurer configurer) {
      configurer.setUseTrailingSlashMatch(true);
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

            objectMapper.registerModule(getGbifSearchModule());
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

    @Bean("occurrenceHeatmapsEsService")
    @ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
    OccurrenceHeatmapsEsService occurrenceHeatmapsEsService(
        @Qualifier("esOccurrenceClient") RestHighLevelClient esClient,
        TileServerConfiguration tileServerConfiguration,
        ConceptClient conceptClient,
        NameUsageMatchingService nameUsageMatchingService,
        @Value("${defaultChecklistKey: 'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c'}") String defaultChecklistKey) {
      return new OccurrenceHeatmapsEsService(
          esClient,
          tileServerConfiguration.getEsOccurrenceConfiguration().getElasticsearch().getIndex(),
          new OccurrenceEsHeatmapRequestBuilder(
              OccurrenceEsField.buildFieldMapper(),
              conceptClient,
              nameUsageMatchingService,
              defaultChecklistKey));
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
        return new HBaseMaps(conf, meta, tileServerConfiguration.getHbase().getSaltModulusPoints(),tileServerConfiguration.getHbase().getSaltModulusTiles(), cacheManager, meterRegistry, pointCacheConfiguration, tileCacheConfiguration);

      } else {
        MapMetastore meta = Metastores.newStaticMapsMeta(tileServerConfiguration.getHbase().getTilesTableName(),
          tileServerConfiguration.getHbase().getPointsTableName());
        return new HBaseMaps(conf, meta, tileServerConfiguration.getHbase().getSaltModulusPoints(),tileServerConfiguration.getHbase().getSaltModulusTiles(), cacheManager, meterRegistry, pointCacheConfiguration, tileCacheConfiguration);
      }
    }

    @Primary
    @Bean
    public ObjectMapper objectMapper() {
      return JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport()
                                            .registerModule(getGbifSearchModule())
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
    @ConditionalOnExpression("${esOccurrenceConfiguration.enabled}")
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

  /** GBIF Search module for Jackson ObjectMapper */
  private static SimpleModule getGbifSearchModule() {
    return new SimpleModule()
      .addKeyDeserializer(
        SearchParameter.class,
        new OccurrenceSearchParameter.OccurrenceSearchParameterKeyDeserializer())
      .addDeserializer(
        SearchParameter.class,
        new OccurrenceSearchParameter.OccurrenceSearchParameterDeserializer())
      .addKeyDeserializer(
        OccurrenceSearchParameter.class,
        new OccurrenceSearchParameter.OccurrenceSearchParameterKeyDeserializer())
      .addDeserializer(
        OccurrenceSearchParameter.class,
        new OccurrenceSearchParameter.OccurrenceSearchParameterDeserializer());
  }
}
