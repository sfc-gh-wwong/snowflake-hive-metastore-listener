import com.snowflake.core.commands.AlterPartition;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({Configuration.class, HiveMetaStore.HMSHandler.class})

/**
 * Tests for generating the alter partition command
 */
public class AlterPartitionTest
{
  /**
   * A basic test for generating an alter partition command for a simple
   * partition
   * @throws Exception
   */
  @Test
  public void basicAlterPartitionGenerateCommandTest() throws Exception
  {
    // mock table
    Table table = createMockTable();

    // mock partition
    Partition partition = new Partition();
    partition.setValues(Arrays.asList("1", "testName"));
    partition.setSd(new StorageDescriptor());
    partition.getSd().setLocation("s3n://bucketname/path/to/table/sub/path");

    HiveMetaStore.HMSHandler mockHandler =
        PowerMockito.mock(HiveMetaStore.HMSHandler.class);

    AlterPartitionEvent alterPartitionEvent =
        new AlterPartitionEvent(partition, partition, table, true,
                                true, mockHandler);

    AlterPartition alterPartition = new AlterPartition(alterPartitionEvent);

    List<String> commands = alterPartition.generateCommands();
    assertEquals("generated add partition command does not match " +
                     "expected add partition command",
                 "ALTER EXTERNAL TABLE t1 ADD PARTITION(partcol='1'," +
                     "name='testName') LOCATION 'sub/path' /* TABLE LOCATION " +
                     "= 's3n://bucketname/path/to/table' */;",
                 commands.get(0));
  }

  private static Table createMockTable()
  {
    Table table = new Table();
    table.setTableName("t1");
    table.setPartitionKeys(Arrays.asList(
        new FieldSchema("partcol", "int", null),
        new FieldSchema("name", "string", null)));
    table.setSd(new StorageDescriptor());
    table.getSd().setLocation("s3n://bucketname/path/to/table");

    return table;
  }
}
