package org.example.service;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.example.config.CassandraDriverConfigLoaderBuilderCustomizer;
import org.example.exception.UserNotFoundException;
import org.example.model.UserAction;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@SpringBootTest(
  properties = {
    "topic-to-consume-message=audit_topic",
    "spring.kafka.consumer.group-id=1",
    "spring.kafka.bootstrap-servers=localhost:29093"
  }
)
@Import({KafkaAutoConfiguration.class, KafkaConsumerServiceTest.ObjectMapperTestConfig.class, CassandraDriverConfigLoaderBuilderCustomizer.class})
@Testcontainers
public class KafkaConsumerServiceTest {

  private static final DockerImageName KAFKA_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-kafka:7.4.0")
    .asCompatibleSubstituteFor("apache/kafka");

  @TestConfiguration
  static class ObjectMapperTestConfig {
    @Bean
    public ObjectMapper objectMapper() {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.registerModule(new JavaTimeModule());
      return objectMapper;
    }
  }

  @Container
  @ServiceConnection
  public static final KafkaContainer KAFKA = new KafkaContainer(KAFKA_TEST_IMAGE);

  @Container
  private static final CassandraContainer<?> cassandraContainer = new CassandraContainer<>("cassandra:3.11.2")
    .withExposedPorts(9042)
    .withInitScript("init.cql");

  @Autowired
  private KafkaConsumerService kafkaConsumerService;
  @Autowired
  private UserAuditService userAuditService;
  @Autowired
  private KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  private ObjectMapper objectMapper;

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
  @DisplayName("This test check consume message method")
  void test1() throws JsonProcessingException, InterruptedException {
    UserAction userAction = new UserAction(
      1, Instant.now(), "INSERT", "none"
    );

    kafkaTemplate.send("audit_topic",
      objectMapper.writeValueAsString(userAction)
    );

    Thread.sleep(5000);

    UserAction cassandraUserAction = userAuditService.readUserAudit(1).get(0);

    assertEquals(userAction.getId(), cassandraUserAction.getId());
    assertEquals(userAction.getEventDetails(), cassandraUserAction.getEventDetails());
    assertEquals(userAction.getEventType(), cassandraUserAction.getEventType());
    assertEquals(userAction.getEventTime().getEpochSecond(), cassandraUserAction.getEventTime().getEpochSecond());
  }

  @Test
  @DisplayName("Negative test checking consume method")
  void test2() throws JsonProcessingException, InterruptedException {
    UserAction userAction = new UserAction(
      1, Instant.now(), "INSERT", "none"
    );

    kafkaTemplate.send("other_topic",
      objectMapper.writeValueAsString(userAction)
    );

    Thread.sleep(5000);

    assertThrows(UserNotFoundException.class, () -> {
      userAuditService.readUserAudit(1);
    });
  }
}
