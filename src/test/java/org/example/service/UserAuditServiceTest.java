package org.example.service;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import org.example.CassandraConnector;
import org.example.config.CassandraConfiguration;
import org.example.config.CassandraDriverConfigLoaderBuilderCustomizer;
import org.example.exception.UserNotFoundException;
import org.example.model.UserAction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;

import static org.example.model.ActionType.INSERT;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@SpringBootTest
@Import({CassandraDriverConfigLoaderBuilderCustomizer.class})
public class UserAuditServiceTest {

  @Container
  private static final CassandraContainer<?> cassandraContainer = new CassandraContainer<>("cassandra:3.11.2")
    .withExposedPorts(9042)
    .withInitScript("init.cql");

  @Autowired
  private UserAuditService userAuditService;

  @BeforeAll
  static void setupCassandraConnectionProperties() {
    System.setProperty("spring.cassandra.keyspace-name", "my_keyspace");
    System.setProperty(
      "spring.cassandra.contact-points", cassandraContainer.getContainerIpAddress());
    System.setProperty(
      "spring.cassandra.port", String.valueOf(cassandraContainer.getMappedPort(9042)));
    System.setProperty(
      "spring.cassandra.local-datacenter", "datacenter1"
    );
    System.setProperty("JAVA_TOOL_OPTIONS", "--add-opens=java.base/java.time=ALL-UNNAMED");
  }

  @Test
  @DisplayName("this test check read user audit by UUID method when user created")
  public void test1() {
    CassandraConnector cassandraConnector = new CassandraConnector();
    CassandraConfiguration cassandraConfiguration = new CassandraConfiguration();
    cassandraConnector.connect(
      System.getProperty("spring.cassandra.contact-points"),
      Integer.parseInt(System.getProperty("spring.cassandra.port")),
      System.getProperty("spring.cassandra.local-datacenter")
    );
    CqlSession session = cassandraConnector.getSession();

    PreparedStatement preparedStatement = session.prepare(
      "INSERT INTO my_keyspace.user_audit (user_id, event_time, event_type, event_details) VALUES (?, ?, ?, ?)"
    );

    UserAction userAction = new UserAction(
      1,
      Instant.now(),
      INSERT.getValue(),
      "insert into db"
    );

    BoundStatement boundStatement = preparedStatement.bind(
      userAction.getId(), userAction.getEventTime(), userAction.getEventType(), userAction.getEventDetails()
    );

    session.execute(boundStatement);

    session.close();

    UserAction userServiceAction = userAuditService.readUserAudit(userAction.getId()).get(0);

    assertEquals(userAction.getId(), userServiceAction.getId());
    assertEquals(userAction.getEventTime().getEpochSecond(), userServiceAction.getEventTime().getEpochSecond());
    assertEquals(userAction.getEventType(), userServiceAction.getEventType());
    assertEquals(userAction.getEventDetails(), userServiceAction.getEventDetails());
  }

  @Test
  @DisplayName("this test check read user audit by UUID method when user is not created")
  public void test2() {
    Assertions.assertThrows(UserNotFoundException.class, () -> {userAuditService.readUserAudit(2);});
  }

  @Test
  @DisplayName("this test check insert user action method")
  public void test3() {
    UserAction userAction = new UserAction(
      3,
      Instant.now(),
      INSERT.getValue(),
      "insert into db"
    );

    userAuditService.insertUserAction(userAction);
    UserAction userServiceAction = userAuditService.readUserAudit(userAction.getId()).get(0);

    assertEquals(userAction.getId(), userServiceAction.getId());
    assertEquals(userAction.getEventTime().getEpochSecond(), userServiceAction.getEventTime().getEpochSecond());
    assertEquals(userAction.getEventType(), userServiceAction.getEventType());
    assertEquals(userAction.getEventDetails(), userServiceAction.getEventDetails());
  }
}