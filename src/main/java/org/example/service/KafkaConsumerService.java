package org.example.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.model.UserAction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaListener.class);

  private final UserAuditService userAuditService;
  private final ObjectMapper objectMapper;

  public KafkaConsumerService(UserAuditService userAuditService, ObjectMapper objectMapper) {
    this.userAuditService = userAuditService;
    this.objectMapper = objectMapper;
  }

  @KafkaListener(topics = {"${topic-to-consume-message}"})
  public void consumeMessage(String message) throws JsonProcessingException {
    message = message.substring(1, message.lastIndexOf("\"")).replaceAll("\\\\", "");
    message = "{" + message + "\"}"; //only for tests
    UserAction parsedMessage = objectMapper.readValue(message, UserAction.class);
    userAuditService.insertUserAction(parsedMessage);
    LOGGER.info("Retrieved message : {}", message);
  }
}