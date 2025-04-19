package org.example.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Value;

import java.time.Instant;

@Value
public class UserAction {
  long id;
  Instant eventTime;
  String eventType;
  String eventDetails;

  @JsonCreator
  public UserAction(
    @JsonProperty("id") long id,
    @JsonProperty("eventTime") Instant eventTime,
    @JsonProperty("eventType") String eventType,
    @JsonProperty("eventDetails") String eventDetails
  ) {
    this.id = id;
    this.eventTime = eventTime;
    this.eventType = eventType;
    this.eventDetails = eventDetails;
  }
}