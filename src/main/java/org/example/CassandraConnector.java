package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.net.InetSocketAddress;

@Getter
@Component
public class CassandraConnector {

  private CqlSession session;

  public void connect(String node, int port, String dataCenter) {
    CqlSessionBuilder builder = CqlSession.builder();
    builder.addContactPoint(new InetSocketAddress(node, port));
    builder.withLocalDatacenter(dataCenter);
    builder.withAuthCredentials("cassandra", "cassandra");
    session = builder.build();
  }

  public void close() {
    session.close();
  }
}
