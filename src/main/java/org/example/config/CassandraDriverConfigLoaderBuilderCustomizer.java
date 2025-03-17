package org.example.config;

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import org.springframework.boot.autoconfigure.cassandra.DriverConfigLoaderBuilderCustomizer;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
public class CassandraDriverConfigLoaderBuilderCustomizer implements DriverConfigLoaderBuilderCustomizer {

  @Override
  public void customize(ProgrammaticDriverConfigLoaderBuilder programmaticDriverConfigLoaderBuilder) {
    programmaticDriverConfigLoaderBuilder
      .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(20))
      .withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT, Duration.ofSeconds(20))
      .withDuration(DefaultDriverOption.CONNECTION_INIT_QUERY_TIMEOUT, Duration.ofSeconds(20));
  }
}
