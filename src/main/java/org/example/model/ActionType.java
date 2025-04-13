package org.example.model;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ActionType {
  INSERT("INSERT"),
  UPDATE("UPDATE"),
  DELETE("DELETE");

  private final String value;
}
