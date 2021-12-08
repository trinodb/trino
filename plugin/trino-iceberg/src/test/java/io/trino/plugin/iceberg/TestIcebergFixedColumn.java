package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.spi.connector.SchemaTableName;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;

import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.IcebergQueryRunner.createIcebergQueryRunner;
import static io.trino.plugin.iceberg.IcebergUtil.loadIcebergTable;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.tpch.TpchTable.NATION;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class TestIcebergFixedColumn
        extends AbstractTestQueryFramework
{
    private HiveMetastore metastore;
    private HdfsEnvironment hdfsEnvironment;
    private File metastoreDir;
    private java.nio.file.Path tempDir;
    private IcebergTableOperationsProvider tableOperationsProvider;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        HdfsConfig config = new HdfsConfig();
        HdfsConfiguration configuration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(config), ImmutableSet.of());
        hdfsEnvironment = new HdfsEnvironment(configuration, config, new NoHdfsAuthentication());
        tempDir = Files.createTempDirectory("test_iceberg");
        metastoreDir = tempDir.resolve("iceberg_data").toFile();
        metastore = createTestingFileHiveMetastore(metastoreDir);
        tableOperationsProvider = new FileMetastoreTableOperationsProvider(new HdfsFileIoProvider(hdfsEnvironment));

        return createIcebergQueryRunner(ImmutableMap.of(), ImmutableMap.of(), ImmutableList.of(NATION), Optional.of(metastoreDir));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws IOException
    {
        deleteRecursively(tempDir, ALLOW_INSECURE);
    }

    @Test
    public void testReadingFromBinaryColumnUsingIceberg() {
        String tableName = "test_binary_with_iceberg";
        assertUpdate(format("CREATE TABLE %s (_integer INTEGER)", tableName));
        BaseTable table = (BaseTable) loadIcebergTable(metastore, tableOperationsProvider, SESSION, new SchemaTableName("tpch", tableName));

        addColumn(table, "_fixed", Types.BinaryType.get());
        addPartitioning(table, "_fixed");

        DataFile data = DataFiles.builder(table.spec())
                .withPath("ignoredWithoutReading")
                .withFileSizeInBytes(410)
                .withFormat("ORC")
                .withMetrics(new Metrics(0L, null, ImmutableMap.of(), ImmutableMap.of()))
                .withPartition(PartitionData.fromJson("{\"partitionValues\":[\"/wD/7v/u\"]}", new Type[]{Types.BinaryType.get()}))
                .build();

        table.newAppend().appendFile(data).commit();
    }

    @Test
    public void testReadingFromBinaryColumnUsingTrino() {
        String tableName = "test_binary_with_trino";
        assertUpdate(format("CREATE TABLE %s (_integer INTEGER)", tableName));
        BaseTable table = (BaseTable) loadIcebergTable(metastore, tableOperationsProvider, SESSION, new SchemaTableName("tpch", tableName));

        addColumn(table, "_fixed", Types.BinaryType.get());
        addPartitioning(table, "_fixed");

        assertUpdate(format("INSERT INTO %s VALUES(1, X'ff00ffeeffee')", tableName), 1);
        assertThat(query(format("SELECT * FROM %s", tableName))).matches("VALUES (1, X'ff00ffeeffee')");
    }

    @Test
    public void testReadingFromFixedColumnWithIceberg_FAILING() {
        String tableName = "test_fixed_with_iceberg";
        assertUpdate(format("CREATE TABLE %s (_integer INTEGER)", tableName));
        BaseTable table = (BaseTable) loadIcebergTable(metastore, tableOperationsProvider, SESSION, new SchemaTableName("tpch", tableName));

        addColumn(table, "_fixed", Types.FixedType.ofLength(6));
        addPartitioning(table, "_fixed");

        DataFile data = DataFiles.builder(table.spec())
                .withPath("ignoredWithoutReading")
                .withFileSizeInBytes(410)
                .withFormat("ORC")
                .withMetrics(new Metrics(0L, null, ImmutableMap.of(), ImmutableMap.of()))
                .withPartition(PartitionData.fromJson("{\"partitionValues\":[\"/wD/7v/u\"]}", new Type[]{Types.FixedType.ofLength(6)}))
                .build();
        assertThatExceptionOfType(DataFileWriter.AppendWriteException.class).isThrownBy(() -> table.newAppend().appendFile(data).commit());
        // Caused by: java.lang.ClassCastException: class java.nio.HeapByteBuffer cannot be cast to class org.apache.avro.generic.GenericData$Fixed (java.nio.HeapByteBuffer is in module java.base of loader 'bootstrap'; org.apache.avro.generic.GenericData$Fixed is in unnamed module of loader 'app')
//        assertUpdate(format("INSERT INTO %s VALUES(1, X'ff00ffeeffee')", tableName), 1);
//        assertThat(query(format("SELECT * FROM %s", tableName))).matches("VALUES (1, X'ff00ff')");
    }

    @Test
    public void testReadingFromFixedColumnWithTrino_FAILING() {
        String tableName = "test_fixed_with_trino";
        assertUpdate(format("CREATE TABLE %s (_integer INTEGER)", tableName));
        BaseTable table = (BaseTable) loadIcebergTable(metastore, tableOperationsProvider, SESSION, new SchemaTableName("tpch", tableName));

        addColumn(table, "_fixed", Types.FixedType.ofLength(6));
        addPartitioning(table, "_fixed");

        assertThatExceptionOfType(RuntimeException.class).isThrownBy(() -> getQueryRunner().execute(format("INSERT INTO %s VALUES(1, X'ff00ffeeffee')", tableName)));
//        assertThat(query(format("SELECT * FROM %s", tableName))).matches("VALUES (1, X'ff00ffeeffee')");
        // Caused by: java.lang.ClassCastException: class java.nio.HeapByteBuffer cannot be cast to class org.apache.avro.generic.GenericData$Fixed (java.nio.HeapByteBuffer is in module java.base of loader 'bootstrap'; org.apache.avro.generic.GenericData$Fixed is in unnamed module of loader 'app')
    }

    private void addColumn(BaseTable table, String columnName, Type columnType) {
        table.updateSchema().addColumn(columnName, columnType).commit();
    }

    private void addPartitioning(BaseTable table, String columnName) {
        TableMetadata currentMetadata = table.operations().current();
        PartitionSpec partitionSpec = PartitionSpec.builderFor(table.schema()).identity(columnName).build();
        TableMetadata newMetadata = table.operations().current().updatePartitionSpec(partitionSpec);
        table.operations().commit(currentMetadata, newMetadata);
    }
}
