package org.example.service;

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
import java.util.List;

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
  @DisplayName("this test check read user audit by UUID method and insert user audit method")
  public void test1() {

    UserAction userAction = new UserAction(
      1,
      Instant.now(),
      INSERT.getValue(),
      "insert into db"
    );

    userAuditService.insertUserAction(userAction);
    List<UserAction> userServiceAction = userAuditService.readUserAudit(userAction.getId());

    assertEquals(userServiceAction.size(), 1);
    assertEquals(userAction.getId(), userServiceAction.get(0).getId());
    assertEquals(userAction.getEventTime().getEpochSecond(), userServiceAction.get(0).getEventTime().getEpochSecond());
    assertEquals(userAction.getEventType(), userServiceAction.get(0).getEventType());
    assertEquals(userAction.getEventDetails(), userServiceAction.get(0).getEventDetails());
  }

  @Test
  @DisplayName("this test check read user audit by UUID method when user is not created")
  public void test2() {
    Assertions.assertThrows(UserNotFoundException.class, () -> {userAuditService.readUserAudit(2);});
  }
}