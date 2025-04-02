package org.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.Instant;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class UserAction {
  @JsonProperty("id")
  private long id;
  @JsonProperty("eventTime")
  private Instant eventTime;
  @JsonProperty("eventType")
  private String eventType;
  @JsonProperty("eventDetails")
  private String eventDetails;
}