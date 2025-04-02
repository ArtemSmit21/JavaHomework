package org.example.models;

import lombok.AllArgsConstructor;
import lombok.Getter;

import java.time.Instant;
import java.util.UUID;

@AllArgsConstructor
@Getter
public class UserAction {
  private UUID id;
  private Instant eventTime;
  private String eventType;
  private String eventDetails;
}
