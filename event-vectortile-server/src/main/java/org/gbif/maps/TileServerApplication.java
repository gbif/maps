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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import javax.validation.constraints.NotNull;
import org.apache.http.HttpHost;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.gbif.api.model.common.search.SearchParameter;
import org.gbif.api.model.event.search.EventSearchParameter;
import org.gbif.api.model.occurrence.search.OccurrenceSearchParameter;
import org.gbif.occurrence.search.es.EsConfig;
import org.gbif.rest.client.species.NameUsageMatchingService;
import org.gbif.search.es.event.EventEsField;
import org.gbif.search.heatmap.es.event.EventEsHeatmapRequestBuilder;
import org.gbif.search.heatmap.es.event.EventHeatmapsEsService;
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

    @Bean("esEventClient")
    @ConditionalOnExpression("${esEventConfiguration.enabled}")
    public RestHighLevelClient provideEventEsClient(TileServerConfiguration tileServerConfiguration) {
      return provideEsClient(tileServerConfiguration.getEsEventConfiguration().getElasticsearch());
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

    @Bean("eventHeatmapsEsService")
    @ConditionalOnExpression("${esEventConfiguration.enabled}")
    EventHeatmapsEsService eventHeatmapsEsService(
        @Qualifier("esEventClient") RestHighLevelClient esClient,
        TileServerConfiguration tileServerConfiguration,
        ConceptClient conceptClient,
        NameUsageMatchingService nameUsageMatchingService,
        @Value("${defaultChecklistKey: 'd7dddbf4-2cf0-4f39-9b2a-bb099caae36c'}") String defaultChecklistKey) {
      return new EventHeatmapsEsService(
          esClient,
          tileServerConfiguration.getEsEventConfiguration().getElasticsearch().getIndex(),
          new EventEsHeatmapRequestBuilder(
              EventEsField.buildFieldMapper(),
              conceptClient,
              nameUsageMatchingService,
              defaultChecklistKey));
    }

    @Primary
    @Bean
    public ObjectMapper objectMapper() {
      return JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport()
                                            .registerModule(getGbifSearchModule())
                                            .registerModule(new JavaTimeModule());
    }

    @Bean
    @ConditionalOnExpression("${esEventConfiguration.enabled}")
    public ConceptClient conceptClient(@Value("${api.url}") String apiUrl) {
      return new ClientBuilder()
        .withObjectMapper(
          JacksonJsonObjectMapperProvider.getObjectMapperWithBuilderSupport()
            .registerModule(new JavaTimeModule()))
        .withUrl(apiUrl)
        .build(ConceptClient.class);
    }

    @Bean
    @ConditionalOnExpression("${esEventConfiguration.enabled}")
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
            SearchParameter.class, new EventSearchParameter.EventSearchParameterKeyDeserializer())
        .addDeserializer(
            EventSearchParameter.class, new EventSearchParameter.EventSearchParameterDeserializer())
        .addKeyDeserializer(
            OccurrenceSearchParameter.class,
            new EventSearchParameter.EventSearchParameterKeyDeserializer())
        .addDeserializer(
            EventSearchParameter.class,
            new EventSearchParameter.EventSearchParameterDeserializer());
  }
}
