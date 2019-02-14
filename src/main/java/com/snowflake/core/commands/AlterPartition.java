/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
package com.snowflake.core.commands;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * A class for the AlterPartition command
 * @author wwong
 */
public class AlterPartition implements Command
{
  /**
   * Creates an AlterPartition command
   * @param alterPartitionEvent Event to generate a command from
   */
  public AlterPartition(AlterPartitionEvent alterPartitionEvent)
  {
    // Unlike add partition events, alter partition events come in one by one
    Preconditions.checkNotNull(alterPartitionEvent);
    this.hiveTable = Preconditions.checkNotNull(alterPartitionEvent.getTable());
    this.oldPartition =
        Preconditions.checkNotNull(alterPartitionEvent.getOldPartition());
    this.newPartition =
        Preconditions.checkNotNull(alterPartitionEvent.getNewPartition());
  }

  /**
   * Generates the necessary commands on a Hive alter partition event
   * @return The Snowflake commands generated
   */
  public List<String> generateCommands()
  {
    // In contrast to Hive, adding existing partitions to Snowflake will not
    // result in an error. Instead, it updates the file registration.
    Supplier<Iterator<Partition>> partitionIterator =
        () -> Collections.singletonList(newPartition).iterator();
    return new AddPartition(hiveTable, partitionIterator).generateCommands();
  }

  private final Table hiveTable;

  private final Partition oldPartition;

  private final Partition newPartition;
}
