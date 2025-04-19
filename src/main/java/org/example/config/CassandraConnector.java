package org.example.config;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetSocketAddress;

@Configuration
public class CassandraConnector {

  @Value("${spring.cassandra.port}")
  private int port;

  @Value("${spring.cassandra.local-datacenter}")
  private String datacenter;

  @Value("${spring.cassandra.contact-points}")
  private String contactPoints;

  @Value("${spring.cassandra.keyspace-name}")
  private String keyspaceName;

  @Bean
  public CqlSession cqlSession() {
    CqlSessionBuilder builder = CqlSession.builder();
    builder.addContactPoint(new InetSocketAddress(contactPoints, port));
    builder.withLocalDatacenter(datacenter);
    builder.withAuthCredentials("cassandra", "cassandra");
    CqlSession cqlSession = builder.build();

    cqlSession.execute(String.format("""
      CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = {
          'class': 'SimpleStrategy',
          'replication_factor': 3
        };
      """, keyspaceName));

    cqlSession.execute(String.format("""
      CREATE TABLE IF NOT EXISTS %s.user_audit (
        user_id BIGINT,
        event_time TIMESTAMP,
        event_type TEXT,
        event_details TEXT,
        PRIMARY KEY ( (user_id), event_time )
      ) WITH CLUSTERING ORDER BY (event_time DESC)
        AND DEFAULT_TIME_TO_LIVE = 31536000;""", keyspaceName));

    return cqlSession;
  }
}
