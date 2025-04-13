package org.example.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import jakarta.annotation.PostConstruct;
import org.example.exception.UserNotFoundException;
import org.example.model.UserAction;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class UserAuditService {

  private final CqlSession cqlSession;

  private PreparedStatement insertUserAction;

  private PreparedStatement selectUserAction;

  public UserAuditService(CqlSession cqlSession) {
    this.cqlSession = cqlSession;
  }

  @PostConstruct
  private void init() {
    insertUserAction = cqlSession.prepare(
      "INSERT INTO my_keyspace.user_audit (user_id, event_time, event_type, event_details) VALUES (?, ?, ?, ?)"
    );
    selectUserAction = cqlSession.prepare(
      "SELECT * FROM my_keyspace.user_audit WHERE user_id = ?"
    );
  }

  public void insertUserAction(UserAction userAction) {
    BoundStatement boundStatement = insertUserAction.bind(
      userAction.getId(), userAction.getEventTime(), userAction.getEventType(), userAction.getEventDetails()
    );
    cqlSession.execute(boundStatement);
  }

  public List<UserAction> readUserAudit(long id) {
    BoundStatement boundStatement = selectUserAction.bind(id);

    ResultSet resultSet = cqlSession.execute(boundStatement);

    List<UserAction> userActions = new ArrayList<>();

    for (Row row : resultSet) {
      userActions.add(new UserAction(
        row.getLong("user_id"), row.getInstant("event_time"),
        row.getString("event_type"), row.getString("event_details")
      ));
    }

    if (userActions.isEmpty()) {
      throw new UserNotFoundException("User with id " + id + " not found");
    }
    return userActions;
  }
}
