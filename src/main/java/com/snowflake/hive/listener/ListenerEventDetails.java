package com.snowflake.hive.listener;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.events.ListenerEvent;

public class ListenerEventDetails
{
  private final String databaseName;

  private final String tableName;

  private final ListenerEvent event;

  public ListenerEventDetails(String databaseName, String tableName,
                              ListenerEvent event)
  {
    Preconditions.checkNotNull(databaseName);
    Preconditions.checkNotNull(tableName);
    Preconditions.checkNotNull(event);
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.event = event;
  }

  public String getDatabaseName()
  {
    return databaseName;
  }

  public String getTableName()
  {
    return tableName;
  }

  public ListenerEvent getEvent()
  {
    return event;
  }
}
