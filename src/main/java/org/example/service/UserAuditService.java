package org.example.service;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import lombok.AllArgsConstructor;
import org.example.CassandraConnector;
import org.example.config.CassandraConfiguration;
import org.example.exception.TestException;
import org.example.exception.UserNotFoundException;
import org.example.model.UserAction;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
@AllArgsConstructor
public class UserAuditService {

  private final CassandraConfiguration cassandraConfiguration;

  public void insertUserAction(UserAction userAction) {
    CassandraConnector cassandraConnector = new CassandraConnector();
    cassandraConnector.connect(
      cassandraConfiguration.getContactPoints(),
      cassandraConfiguration.getPort(),
      cassandraConfiguration.getLocalDatacenter()
    );
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

  public List<UserAction> readUserAudit(long id) {
    CassandraConnector cassandraConnector = new CassandraConnector();
    cassandraConnector.connect(
      cassandraConfiguration.getContactPoints(),
      cassandraConfiguration.getPort(),
      cassandraConfiguration.getLocalDatacenter()
    );
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
