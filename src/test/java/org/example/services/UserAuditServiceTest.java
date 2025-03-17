package org.example.services;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.CqlSession;
import org.example.config.CassandraConnector;
import org.example.exceptions.UserNotFoundException;
import org.example.models.UserAction;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.UUID;

import static org.example.models.ActionType.INSERT;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@SpringBootTest
public class UserAuditServiceTest {

  @Container
  private static final CassandraContainer<?> cassandraContainer = new CassandraContainer<>("cassandra:3.11.2")
    .withExposedPorts(9042)
    .withInitScript("init.cql");

  @Autowired
  private UserAuditService userAuditService;

  @Test
  @DisplayName("this test check read user audit by UUID method when user created")
  public void test1() {
    CassandraConnector cassandraConnector = new CassandraConnector();
    cassandraConnector.connect("127.0.0.1", 9042, "datacenter1");
    CqlSession session = cassandraConnector.getSession();

    PreparedStatement preparedStatement = session.prepare(
      "INSERT INTO my_keyspace.user_audit (user_id, event_time, event_type, event_details) VALUES (?, ?, ?, ?)"
    );

    UserAction userAction = new UserAction(
      UUID.randomUUID(),
      Instant.now(),
      INSERT.getValue(),
      "insert into db"
    );

    BoundStatement boundStatement = preparedStatement.bind(
      userAction.getId(), userAction.getEventTime(), userAction.getEventType(), userAction.getEventDetails()
    );

    session.execute(boundStatement);

    UserAction userServiceAction = userAuditService.readUserAudit(userAction.getId()).get(0);

    assertEquals(userAction.getId(), userServiceAction.getId());
    assertEquals(userAction.getEventTime().getEpochSecond(), userServiceAction.getEventTime().getEpochSecond());
    assertEquals(userAction.getEventType(), userServiceAction.getEventType());
    assertEquals(userAction.getEventDetails(), userServiceAction.getEventDetails());
  }

  @Test
  @DisplayName("this test check read user audit by UUID method when user is not created")
  public void test2() {
    Assertions.assertThrows(UserNotFoundException.class, () -> {userAuditService.readUserAudit(UUID.randomUUID());});
  }

  @Test
  @DisplayName("this test check insert user action method")
  public void test3() {
    UserAction userAction = new UserAction(
      UUID.randomUUID(),
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