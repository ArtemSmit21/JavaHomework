package org.example.services;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import lombok.AllArgsConstructor;
import org.example.CassandraConnector;
import org.example.exceptions.UserNotFoundException;
import org.example.models.UserAction;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@AllArgsConstructor
public class UserAuditService {

  private final CassandraConnector cassandraConnector;

  public void insertUserAction(UserAction userAction) {
    cassandraConnector.connect("127.0.0.1", 9042, "datacenter1");
    CqlSession session = cassandraConnector.getSession();

    PreparedStatement preparedStatement = session.prepare(
      "INSERT INTO my_keyspace.user_audit (user_id, event_time, event_type, event_details) VALUES (?, ?, ?, ?)"
    );

    BoundStatement boundStatement = preparedStatement.bind(
      userAction.getId(), userAction.getEventTime(), userAction.getEventType(), userAction.getEventDetails()
    );

    session.execute(boundStatement);

    session.close();
  }

  public List<UserAction> readUserAudit(UUID id) {
    cassandraConnector.connect("127.0.0.1", 9042, "datacenter1");
    CqlSession session = cassandraConnector.getSession();

    PreparedStatement preparedStatement = session.prepare(
      "SELECT * FROM my_keyspace.user_audit WHERE user_id = ?"
    );

    BoundStatement boundStatement = preparedStatement.bind(id);

    ResultSet resultSet = session.execute(boundStatement);

    session.close();

    List<UserAction> userActions = new ArrayList<>();

    for (Row row : resultSet) {
      userActions.add(new UserAction(
        row.getUuid("user_id"), row.getInstant("event_time"),
        row.getString("event_type"), row.getString("event_details")
      ));
    }

    if (userActions.isEmpty()) {
      throw new UserNotFoundException("User with UUID " + id + " not found");
    }
    return userActions;
  }
}
