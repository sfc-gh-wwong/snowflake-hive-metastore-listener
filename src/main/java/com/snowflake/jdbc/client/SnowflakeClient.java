/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.jdbc.client;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.snowflake.conf.SnowflakeConf;
import com.snowflake.core.commands.Command;
import com.snowflake.core.commands.LogCommand;
import com.snowflake.core.util.CommandGenerator;
import com.snowflake.core.util.BatchScheduler;
import com.snowflake.hive.listener.ListenerEventDetails;
import com.snowflake.hive.listener.SnowflakeHiveListener;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Class that uses the snowflake jdbc to connect to snowflake.
 * Executes the commands
 * TODO: modify exception handling
 */
public class SnowflakeClient
{
  private static final Logger log =
      LoggerFactory.getLogger(SnowflakeHiveListener.class);

  private static BatchScheduler<TableKey, ListenerEventDetails> scheduler;

  /**
   * Creates and executes an event for snowflake. The steps are:
   * 1. Generate the list of Snowflake queries that will need to be run given
   *    the Hive command
   * 2. Queue a query to Snowflake
   *
   * After a predetermined delay, the queued queries will be executed.
   * This includes:
   * 1. Batching similar queries
   * 2. Get the connection to a Snowflake account
   * 3. Run the queries on Snowflake
   * @param eventDetails - the hive event details
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   */
  public static void createAndExecuteEventForSnowflake(
      ListenerEventDetails eventDetails,
      SnowflakeConf snowflakeConf)
  {
    boolean backgroundTaskEnabled = !snowflakeConf.getBoolean(
        SnowflakeConf.ConfVars.SNOWFLAKE_CLIENT_FORCE_SYNCHRONOUS.getVarname(), false);
    if (backgroundTaskEnabled)
    {
      initScheduler(snowflakeConf);
      scheduler.enqueueMessage(eventDetails);
    }
    else
    {
      generateAndExecuteSnowflakeCommands(eventDetails.getEvent(), snowflakeConf);
    }
  }

  /**
   * Helper method. Generates commands for an event and executes those commands.
   * @param event - the hive event
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   */
  private static void generateAndExecuteSnowflakeCommands(
      ListenerEvent event,
      SnowflakeConf snowflakeConf)
  {
    // Obtains the proper command
    log.info("Creating the Snowflake command");
    Command command = CommandGenerator.getCommand(event, snowflakeConf);

    // Generate the string queries for the command
    // Some Hive commands require more than one statement in Snowflake
    // For example, for create table, a stage must be created before the table
    log.info("Generating Snowflake queries");
    List<String> commandList;
    try
    {
      commandList = command.generateCommands();
    }
    catch (Exception e)
    {
      log.error("Could not generate the Snowflake commands: " + e.getMessage());

      // Log a message to Snowflake with the error instead
      commandList = new LogCommand(e).generateCommands();
    }

    executeStatements(commandList, snowflakeConf);
  }

  /**
   * Helper method to connect to Snowflake and execute a list of queries
   * @param commandList - The list of queries to execute
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  public static void executeStatements(List<String> commandList,
                                        SnowflakeConf snowflakeConf)
  {
    log.info("Executing statements: " + String.join(", ", commandList));

    // Get connection
    log.info("Getting connection to the Snowflake");
    try (Connection connection = retry(
        () -> getConnection(snowflakeConf), snowflakeConf))
    {
      commandList.forEach(commandStr ->
      {
        try (Statement statement =
            retry(connection::createStatement, snowflakeConf))
        {
          log.info("Executing command: " + commandStr);
          ResultSet resultSet = retry(
              () -> statement.executeQuery(commandStr), snowflakeConf);
          StringBuilder sb = new StringBuilder();
          sb.append("Result:\n");
          while (resultSet.next())
          {
            for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++)
            {
              if (i == resultSet.getMetaData().getColumnCount())
              {
                sb.append(resultSet.getString(i));
              }
              else
              {
                sb.append(resultSet.getString(i));
                sb.append("|");
              }
            }
            sb.append("\n");
          }
          log.info(sb.toString());
        }
        catch (Exception e)
        {
          log.error("There was an error executing the statement: " +
                        e.getMessage());
        }
      });
    }
    catch (java.sql.SQLException e)
    {
      log.error("There was an error creating the query: " +
                    e.getMessage());
    }
  }

  /**
   * (Deprecated)
   * Utility method to connect to Snowflake and execute a query.
   * @param commandStr - The query to execute
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @return The result of the executed query
   * @throws SQLException Thrown if there was an error executing the
   *                      statement or forming a connection.
   */
  public static ResultSet executeStatement(String commandStr,
                                           SnowflakeConf snowflakeConf)
      throws SQLException
  {
    try (Connection connection = retry(() -> getConnection(snowflakeConf),
                                       snowflakeConf);
        Statement statement = retry(connection::createStatement,
                                    snowflakeConf))
    {
      log.info("Executing command: " + commandStr);
      ResultSet resultSet = retry(() -> statement.executeQuery(commandStr),
                                  snowflakeConf);
      log.info("Command successfully executed");
      return resultSet;
    }
    catch (SQLException e)
    {
      log.info("There was an error executing this statement or forming a " +
                   "connection: " + e.getMessage());
      throw e;
    }
  }

  /**
   * Helper method. Initializes and starts the query scheduler
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   */
  private static void initScheduler(SnowflakeConf snowflakeConf)
  {
    if (scheduler != null)
    {
      return;
    }

    int numThreads = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_CLIENT_THREAD_COUNT.getVarname(), 8);

    scheduler = new BatchScheduler<TableKey, ListenerEventDetails>(numThreads)
    {
      @Override
      public boolean processMessages(BlockingQueue<ListenerEventDetails> messages)
      {
        try
        {
          return SnowflakeClient.processMessages(messages, snowflakeConf);
        }
        catch (InterruptedException e)
        {
          log.warn("Thread interrupted: " + e);
          Thread.currentThread().interrupt();
          return false;
        }
      }

      @Override
      public TableKey getKeyFromMessage(ListenerEventDetails message)
      {
        return new TableKey(message.getDatabaseName(), message.getTableName());
      }
    };
  }

  private static boolean processMessages(BlockingQueue<ListenerEventDetails> messages,
                                         SnowflakeConf snowflakeConf)
      throws InterruptedException
  {
    if (messages.isEmpty())
    {
      return false;
    }

    ListenerEventDetails firstEvent = messages.peek();
    log.info("Processing queue for %s.%s...",
             firstEvent.getDatabaseName(),
             firstEvent.getTableName());

    List<Partition> partitionsAddedOrDropped = Lists.newArrayList();
    int commandsExecuted = 0; // Execute N statements on a table at a time.
    while(!messages.isEmpty())
    {
      ListenerEventDetails eventDetails = messages.poll(10, TimeUnit.SECONDS);
      if (eventDetails == null)
      {
        return false;
      }

      ListenerEvent event = eventDetails.getEvent();
      if (event instanceof AddPartitionEvent)
      {
        ((AddPartitionEvent)event).getPartitionIterator()
            .forEachRemaining(partitionsAddedOrDropped::add);
      }
      if (event instanceof AlterPartitionEvent)
      {
        partitionsAddedOrDropped.add(((AlterPartitionEvent)event).getNewPartition());
      }

      if ((event instanceof AddPartitionEvent || event instanceof AlterPartitionEvent))
      {
        ListenerEventDetails nextEventDetails = messages.peek();
        if (!messages.isEmpty()
            && (nextEventDetails.getEvent() instanceof AddPartitionEvent || nextEventDetails.getEvent() instanceof AlterPartitionEvent))
        {
          continue;
        }

        Table table = event instanceof AddPartitionEvent
            ? ((AddPartitionEvent) event).getTable()
            : ((AlterPartitionEvent) event).getTable();
        event = new AddPartitionEvent(table,
                                      partitionsAddedOrDropped,
                                      true,
                                      event.getIHMSHandler());
      }

      generateAndExecuteSnowflakeCommands(event, snowflakeConf);
      commandsExecuted++;
      if (commandsExecuted > 10)
      {
        return true;
      }
    }

    log.info("Queue processed.");
    return false;
  }

  /**
   * Get the connection to the Snowflake account.
   * First finds a Snowflake driver and connects to Snowflake using the
   * given properties.
   * @param snowflakeConf - the configuration for Snowflake Hive metastore
   *                        listener
   * @return The JDBC connection
   * @throws SQLException Exception thrown when initializing the connection
   */
  private static Connection getConnection(SnowflakeConf snowflakeConf)
      throws SQLException
  {
    try
    {
      Class.forName("com.snowflake.jdbc.client.SnowflakeClient");
    }
    catch(ClassNotFoundException e)
    {
      log.error("Driver not found");
    }

    // build connection properties
    Properties properties = new Properties();

    snowflakeConf.forEach(conf ->
      {
        if (!conf.getKey().startsWith("snowflake.jdbc"))
        {
          return;
        }

        SnowflakeConf.ConfVars confVar =
            SnowflakeConf.ConfVars.findByName(conf.getKey());
        if (confVar == null)
        {
          properties.put(conf.getKey(), conf.getValue());
        }
        else
        {
          properties.put(confVar.getSnowflakePropertyName(), conf.getValue());
        }

      });
    String connectStr = snowflakeConf.get(
        SnowflakeConf.ConfVars.SNOWFLAKE_JDBC_CONNECTION.getVarname());

    return DriverManager.getConnection(connectStr, properties);
  }

  /**
   * Helper interface that represents a Supplier that can throw an exception.
   * @param <T> The type of object returned by the supplier
   * @param <E> The type of exception thrown by the supplier
   */
  @FunctionalInterface
  public interface ThrowableSupplier<T, E extends Throwable>
  {
    T get() throws E;
  }

  /**
   * Helper method for simple retries.
   * Note: The total number of attempts is 1 + retries.
   * @param <T> The type of object returned by the supplier
   * @param <E> The type of exception thrown by the supplier
   * @param method The method to be executed and retried on.
   * @param maxRetries The maximum number of retries.
   * @param timeoutInMilliseconds Time between retries.
   */
  private static <T, E extends Throwable> T retry(
      ThrowableSupplier<T,E> method,
      int maxRetries,
      int timeoutInMilliseconds)
  throws E
  {
    // Attempt to call the method with N-1 retries
    for (int i = 0; i < maxRetries; i++)
    {
      try
      {
        // Attempt to call the method
        return method.get();
      }
      catch (Exception e)
      {
        // Wait between retries
        try
        {
          Thread.sleep(timeoutInMilliseconds);
        }
        catch (InterruptedException interruptedEx)
        {
          log.error("Thread interrupted.");
          Thread.currentThread().interrupt();
        }
      }
    }

    // Retry one last time, the exception will by handled by the caller
    return method.get();
  }

  /**
   * Helper method for simple retries. Overload for default arguments.
   * @param <T> The type of object returned by the supplier
   * @param <E> The type of exception thrown by the supplier
   * @param method The method to be executed and retried on.
   */
  private static <T, E extends Throwable> T retry(
      ThrowableSupplier<T, E> method,
      SnowflakeConf snowflakeConf)
  throws E
  {
    int maxRetries = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_HIVEMETASTORELISTENER_RETRY_COUNT.getVarname(), 3);
    int timeoutInMilliseconds = snowflakeConf.getInt(
        SnowflakeConf.ConfVars.SNOWFLAKE_HIVEMETASTORELISTENER_RETRY_TIMEOUT_MILLISECONDS.getVarname(), 1000);
    return retry(method, maxRetries, timeoutInMilliseconds);
  }

  private static class TableKey
  {
    private final String databaseName;

    private final String tableName;

    TableKey(String databaseName, String tableName)
    {
      Preconditions.checkNotNull(databaseName);
      Preconditions.checkNotNull(tableName);
      this.databaseName = databaseName;
      this.tableName = tableName;
    }

    @Override
    public int hashCode()
    {
      return new HashCodeBuilder().append(databaseName).append(tableName).build();
    }

    public String toString()
    {
      return String.format("%s.%s", databaseName, tableName);
    }
  }
}
