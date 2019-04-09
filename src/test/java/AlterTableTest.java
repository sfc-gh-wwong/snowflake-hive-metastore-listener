/*
 * Copyright (c) 2018 Snowflake Computing Inc. All right reserved.
 */
import com.snowflake.core.commands.AlterTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests for generating the alter table command
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class})
public class AlterTableTest
{
  /**
   * A basic test for generating a alter (touch) table command
   * @throws Exception
   */
  @Test
  public void basicTouchTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();
    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();
    AlterTableEvent alterTableEvent = new AlterTableEvent(table, table,
                                                          true, true, mockHandler);

    AlterTable alterTable = new AlterTable(alterTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = alterTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE STAGE IF NOT EXISTS someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated alter table command does not match " +
                     "expected alter table command",
                 "CREATE EXTERNAL TABLE IF NOT EXISTS t1" +
                     "(partcol INT as (parse_json(metadata$external_table_partition):PARTCOL::INT)," +
                     "name STRING as (parse_json(metadata$external_table_partition):NAME::STRING))" +
                     "partition by (partcol,name)" +
                     "partition_type=user_specified location=@someDB__t1 " +
                     "file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
  }

  /**
   * A test for generating a alter (touch) table command
   * Expects an implicitly partitioned Snowflake external table and a refresh
   * statement.
   * @throws Exception
   */
  @Test
  public void unpartitionedTouchTableGenerateCommandTest() throws Exception
  {
    Table table = TestUtil.initializeMockTable();
    table.setPartitionKeys(new ArrayList<>());
    table.getSd().setCols(Arrays.asList(
        new FieldSchema("col1", "int", null),
        new FieldSchema("col2", "string", null)));
    HiveMetaStore.HMSHandler mockHandler = TestUtil.initializeMockHMSHandler();
    AlterTableEvent alterTableEvent = new AlterTableEvent(table, table,
                                                          true, true, mockHandler);

    AlterTable alterTable = new AlterTable(alterTableEvent, TestUtil.initializeMockConfig());

    List<String> commands = alterTable.generateCommands();
    assertEquals("generated create stage command does not match " +
                     "expected create stage command",
                 "CREATE STAGE IF NOT EXISTS someDB__t1 " +
                     "URL='s3://bucketname/path/to/table'\n" +
                     "credentials=(AWS_KEY_ID='accessKeyId'\n" +
                     "AWS_SECRET_KEY='awsSecretKey');",
                 commands.get(0));

    assertEquals("generated alter table command does not match " +
                     "expected alter table command",
                 "CREATE EXTERNAL TABLE IF NOT EXISTS t1(" +
                     "col1 INT as (VALUE:c1::INT),col2 STRING as (VALUE:c2::STRING))" +
                     "partition_type=implicit location=@someDB__t1 " +
                     "file_format=(TYPE=CSV) AUTO_REFRESH=false;",
                 commands.get(1));
    assertEquals("generated alter table command does not match " +
                     "expected alter table command",
                 "ALTER EXTERNAL TABLE t1 REFRESH;",
                 commands.get(2));
  }
}
