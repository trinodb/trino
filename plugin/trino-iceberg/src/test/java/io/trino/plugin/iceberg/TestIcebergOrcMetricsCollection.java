/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.base.CatalogName;
import io.trino.plugin.hive.TrinoViewHiveMetastore;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.plugin.iceberg.catalog.IcebergTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.file.FileMetastoreTableOperationsProvider;
import io.trino.plugin.iceberg.catalog.file.TestingIcebergFileMetastoreCatalogModule;
import io.trino.plugin.iceberg.catalog.hms.TrinoHiveCatalog;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TestingTypeManager;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorSession;
import org.apache.iceberg.FileContent;
import org.apache.iceberg.Table;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.util.Modules.EMPTY_MODULE;
import static io.trino.SystemSessionProperties.MAX_DRIVERS_PER_TASK;
import static io.trino.SystemSessionProperties.TASK_CONCURRENCY;
import static io.trino.SystemSessionProperties.TASK_MAX_WRITER_COUNT;
import static io.trino.SystemSessionProperties.TASK_MIN_WRITER_COUNT;
import static io.trino.plugin.hive.metastore.cache.CachingHiveMetastore.createPerTransactionCache;
import static io.trino.plugin.hive.metastore.file.TestingFileHiveMetastore.createTestingFileHiveMetastore;
import static io.trino.plugin.iceberg.DataFileRecord.toDataFileRecord;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static io.trino.plugin.iceberg.IcebergTestUtils.getFileSystemFactory;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestIcebergOrcMetricsCollection
        extends AbstractTestQueryFramework
{
    private TrinoCatalog trinoCatalog;
    private IcebergTableOperationsProvider tableOperationsProvider;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("iceberg")
                .setSchema("test_schema")
                .setSystemProperty(TASK_CONCURRENCY, "1")
                .setSystemProperty(TASK_MIN_WRITER_COUNT, "1")
                .setSystemProperty(TASK_MAX_WRITER_COUNT, "1")
                .setSystemProperty(MAX_DRIVERS_PER_TASK, "1")
                .setCatalogSessionProperty("iceberg", "orc_string_statistics_limit", Integer.MAX_VALUE + "B")
                .build();
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setNodeCount(1)
                .build();

        File baseDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data").toFile();
        HiveMetastore metastore = createTestingFileHiveMetastore(baseDir);

        queryRunner.installPlugin(new TestingIcebergPlugin(Optional.of(new TestingIcebergFileMetastoreCatalogModule(metastore)), Optional.empty(), EMPTY_MODULE));
        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.of("iceberg.file-format", "ORC"));

        TrinoFileSystemFactory fileSystemFactory = getFileSystemFactory(queryRunner);
        tableOperationsProvider = new FileMetastoreTableOperationsProvider(fileSystemFactory);
        CachingHiveMetastore cachingHiveMetastore = createPerTransactionCache(metastore, 1000);
        trinoCatalog = new TrinoHiveCatalog(
                new CatalogName("catalog"),
                cachingHiveMetastore,
                new TrinoViewHiveMetastore(cachingHiveMetastore, false, "trino-version", "test"),
                fileSystemFactory,
                new TestingTypeManager(),
                tableOperationsProvider,
                false,
                false,
                false,
                new IcebergConfig().isHideMaterializedViewStorageTable());

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.execute("CREATE SCHEMA test_schema");

        return queryRunner;
    }

    @Test
    public void testMetrics()
    {
        assertUpdate("create table no_metrics (c1 varchar, c2 varchar)");
        Table table = IcebergUtil.loadIcebergTable(trinoCatalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("test_schema", "no_metrics"));
        // skip metrics for all columns
        table.updateProperties().set("write.metadata.metrics.default", "none").commit();
        // add one row
        assertUpdate("insert into no_metrics values ('abcd', 'a')", 1);
        List<MaterializedRow> materializedRows = computeActual("select * from \"no_metrics$files\"").getMaterializedRows();
        DataFileRecord datafile = toDataFileRecord(materializedRows.get(0));
        assertThat(datafile.getRecordCount()).isEqualTo(1);
        assertThat(datafile.getValueCounts()).isNull();
        assertThat(datafile.getNullValueCounts()).isNull();
        assertThat(datafile.getUpperBounds()).isNull();
        assertThat(datafile.getLowerBounds()).isNull();
        assertThat(datafile.getColumnSizes()).isNull();

        // keep c1 metrics
        assertUpdate("create table c1_metrics (c1 varchar, c2 varchar)");
        table = IcebergUtil.loadIcebergTable(trinoCatalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("test_schema", "c1_metrics"));
        table.updateProperties()
                .set("write.metadata.metrics.default", "none")
                .set("write.metadata.metrics.column.c1", "full")
                .commit();

        assertUpdate("insert into c1_metrics values ('b', 'a')", 1);
        materializedRows = computeActual("select * from \"c1_metrics$files\"").getMaterializedRows();
        datafile = toDataFileRecord(materializedRows.get(0));
        assertThat(datafile.getRecordCount()).isEqualTo(1);
        assertThat(datafile.getValueCounts().size()).isEqualTo(1);
        assertThat(datafile.getNullValueCounts().size()).isEqualTo(1);
        assertThat(datafile.getUpperBounds().size()).isEqualTo(1);
        assertThat(datafile.getLowerBounds().size()).isEqualTo(1);

        // set c1 metrics mode to count
        assertUpdate("create table c1_metrics_count (c1 varchar, c2 varchar)");
        table = IcebergUtil.loadIcebergTable(trinoCatalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("test_schema", "c1_metrics_count"));
        table.updateProperties()
                .set("write.metadata.metrics.default", "none")
                .set("write.metadata.metrics.column.c1", "counts")
                .commit();

        assertUpdate("insert into c1_metrics_count values ('b', 'a')", 1);
        materializedRows = computeActual("select * from \"c1_metrics_count$files\"").getMaterializedRows();
        datafile = toDataFileRecord(materializedRows.get(0));
        assertThat(datafile.getRecordCount()).isEqualTo(1);
        assertThat(datafile.getValueCounts().size()).isEqualTo(1);
        assertThat(datafile.getNullValueCounts().size()).isEqualTo(1);
        assertThat(datafile.getUpperBounds()).isNull();
        assertThat(datafile.getLowerBounds()).isNull();

        // set c1 metrics mode to truncate(10)
        assertUpdate("create table c1_metrics_truncate (c1 varchar, c2 varchar)");
        table = IcebergUtil.loadIcebergTable(trinoCatalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("test_schema", "c1_metrics_truncate"));
        table.updateProperties()
                .set("write.metadata.metrics.default", "none")
                .set("write.metadata.metrics.column.c1", "truncate(10)")
                .commit();

        assertUpdate("insert into c1_metrics_truncate values ('abcaabcaabcaabca', 'a')", 1);
        materializedRows = computeActual("select * from \"c1_metrics_truncate$files\"").getMaterializedRows();
        datafile = toDataFileRecord(materializedRows.get(0));
        assertThat(datafile.getRecordCount()).isEqualTo(1);
        assertThat(datafile.getValueCounts().size()).isEqualTo(1);
        assertThat(datafile.getNullValueCounts().size()).isEqualTo(1);
        datafile.getUpperBounds().forEach((k, v) -> {
            assertThat(v.length()).isEqualTo(10); });
        datafile.getLowerBounds().forEach((k, v) -> {
            assertThat(v.length()).isEqualTo(10); });

        // keep both c1 and c2 metrics
        assertUpdate("create table c_metrics (c1 varchar, c2 varchar)");
        table = IcebergUtil.loadIcebergTable(trinoCatalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("test_schema", "c_metrics"));
        table.updateProperties()
                .set("write.metadata.metrics.column.c1", "full")
                .set("write.metadata.metrics.column.c2", "full")
                .commit();
        assertUpdate("insert into c_metrics values ('b', 'a')", 1);
        materializedRows = computeActual("select * from \"c_metrics$files\"").getMaterializedRows();
        datafile = toDataFileRecord(materializedRows.get(0));
        assertThat(datafile.getRecordCount()).isEqualTo(1);
        assertThat(datafile.getValueCounts().size()).isEqualTo(2);
        assertThat(datafile.getNullValueCounts().size()).isEqualTo(2);
        assertThat(datafile.getUpperBounds().size()).isEqualTo(2);
        assertThat(datafile.getLowerBounds().size()).isEqualTo(2);

        // keep all metrics
        assertUpdate("create table metrics (c1 varchar, c2 varchar)");
        table = IcebergUtil.loadIcebergTable(trinoCatalog, tableOperationsProvider, TestingConnectorSession.SESSION,
                new SchemaTableName("test_schema", "metrics"));
        table.updateProperties()
                .set("write.metadata.metrics.default", "full")
                .commit();
        assertUpdate("insert into metrics values ('b', 'a')", 1);
        materializedRows = computeActual("select * from \"metrics$files\"").getMaterializedRows();
        datafile = toDataFileRecord(materializedRows.get(0));
        assertThat(datafile.getRecordCount()).isEqualTo(1);
        assertThat(datafile.getValueCounts().size()).isEqualTo(2);
        assertThat(datafile.getNullValueCounts().size()).isEqualTo(2);
        assertThat(datafile.getUpperBounds().size()).isEqualTo(2);
        assertThat(datafile.getLowerBounds().size()).isEqualTo(2);
    }

    @Test
    public void testBasic()
    {
        assertUpdate("CREATE TABLE orders WITH (format = 'ORC') AS SELECT * FROM tpch.tiny.orders", 15000);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"orders$files\"");
        assertThat(materializedResult.getRowCount()).isEqualTo(1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        // check content
        assertThat(datafile.getContent()).isEqualTo(FileContent.DATA.id());

        // Check file format
        assertThat(datafile.getFileFormat()).isEqualTo("ORC");

        // Check file row count
        assertThat(datafile.getRecordCount()).isEqualTo(15000L);

        // Check per-column value count
        datafile.getValueCounts().values().forEach(valueCount -> assertThat(valueCount).isEqualTo((Long) 15000L));

        // Check per-column null value count
        datafile.getNullValueCounts().values().forEach(nullValueCount -> assertThat(nullValueCount).isEqualTo((Long) 0L));

        // Check NaN value count
        // TODO: add more checks after NaN info is collected
        assertThat(datafile.getNanValueCounts()).isNull();

        // Check per-column lower bound
        Map<Integer, String> lowerBounds = datafile.getLowerBounds();
        assertThat(lowerBounds.get(1)).isEqualTo("1");
        assertThat(lowerBounds.get(2)).isEqualTo("1");
        assertThat(lowerBounds.get(3)).isEqualTo("F");
        assertThat(lowerBounds.get(4)).isEqualTo("874.89");
        assertThat(lowerBounds.get(5)).isEqualTo("1992-01-01");
        assertThat(lowerBounds.get(6)).isEqualTo("1-URGENT");
        assertThat(lowerBounds.get(7)).isEqualTo("Clerk#000000001");
        assertThat(lowerBounds.get(8)).isEqualTo("0");
        assertThat(lowerBounds.get(9)).isEqualTo(" about the accou");

        // Check per-column upper bound
        Map<Integer, String> upperBounds = datafile.getUpperBounds();
        assertThat(upperBounds.get(1)).isEqualTo("60000");
        assertThat(upperBounds.get(2)).isEqualTo("1499");
        assertThat(upperBounds.get(3)).isEqualTo("P");
        assertThat(upperBounds.get(4)).isEqualTo("466001.28");
        assertThat(upperBounds.get(5)).isEqualTo("1998-08-02");
        assertThat(upperBounds.get(6)).isEqualTo("5-LOW");
        assertThat(upperBounds.get(7)).isEqualTo("Clerk#000001000");
        assertThat(upperBounds.get(8)).isEqualTo("0");
        assertThat(upperBounds.get(9)).isEqualTo("zzle. carefully!");

        assertUpdate("DROP TABLE orders");
    }

    @Test
    public void testWithNulls()
    {
        assertUpdate("CREATE TABLE test_with_nulls (_integer INTEGER, _real REAL, _string VARCHAR, _timestamp TIMESTAMP(6))");
        assertUpdate("INSERT INTO test_with_nulls VALUES " +
                "(7, 3.4, 'aaa', TIMESTAMP '2020-01-01 00:00:00.123456')," +
                "(3, 4.5, 'bbb', TIMESTAMP '2021-02-01 00:23:10.398102')," +
                "(4, null, 'ccc', null)," +
                "(null, null, 'ddd', null)", 4);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_with_nulls$files\"");
        assertThat(materializedResult.getRowCount()).isEqualTo(1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        // Check per-column value count
        datafile.getValueCounts().values().forEach(valueCount -> assertThat(valueCount).isEqualTo((Long) 4L));

        // Check per-column null value count
        assertThat(datafile.getNullValueCounts().get(1)).isEqualTo((Long) 1L);
        assertThat(datafile.getNullValueCounts().get(2)).isEqualTo((Long) 2L);
        assertThat(datafile.getNullValueCounts().get(3)).isEqualTo((Long) 0L);
        assertThat(datafile.getNullValueCounts().get(4)).isEqualTo((Long) 2L);

        // Check per-column lower bound
        assertThat(datafile.getLowerBounds().get(1)).isEqualTo("3");
        assertThat(datafile.getLowerBounds().get(2)).isEqualTo("3.4");
        assertThat(datafile.getLowerBounds().get(3)).isEqualTo("aaa");
        assertThat(datafile.getLowerBounds().get(4)).isEqualTo("2020-01-01T00:00:00.123");

        assertUpdate("DROP TABLE test_with_nulls");

        assertUpdate("CREATE TABLE test_all_nulls (_integer INTEGER)");
        assertUpdate("INSERT INTO test_all_nulls VALUES null, null, null", 3);
        materializedResult = computeActual("SELECT * FROM \"test_all_nulls$files\"");
        assertThat(materializedResult.getRowCount()).isEqualTo(1);
        datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        // Check per-column value count
        assertThat(datafile.getValueCounts().get(1)).isEqualTo((Long) 3L);

        // Check per-column null value count
        assertThat(datafile.getNullValueCounts().get(1)).isEqualTo((Long) 3L);

        // Check that lower bounds and upper bounds are nulls. (There's no non-null record)
        assertThat(datafile.getLowerBounds()).isNull();
        assertThat(datafile.getUpperBounds()).isNull();

        assertUpdate("DROP TABLE test_all_nulls");
    }

    @Test
    public void testWithNaNs()
    {
        assertUpdate("CREATE TABLE test_with_nans (_int INTEGER, _real REAL, _double DOUBLE)");
        assertUpdate("INSERT INTO test_with_nans VALUES (1, 1.1, 1.1), (2, nan(), 4.5), (3, 4.6, -nan())", 3);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_with_nans$files\"");
        assertThat(materializedResult.getRowCount()).isEqualTo(1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        // Check per-column value count
        datafile.getValueCounts().values().forEach(valueCount -> assertThat(valueCount).isEqualTo((Long) 3L));

        // Check per-column nan value count
        assertThat(datafile.getNanValueCounts().size()).isEqualTo(2);
        assertThat(datafile.getNanValueCounts().get(2)).isEqualTo((Long) 1L);
        assertThat(datafile.getNanValueCounts().get(3)).isEqualTo((Long) 1L);

        assertThat(datafile.getLowerBounds().get(2)).isNull();
        assertThat(datafile.getLowerBounds().get(3)).isNull();
        assertThat(datafile.getUpperBounds().get(2)).isNull();
        assertThat(datafile.getUpperBounds().get(3)).isNull();

        assertUpdate("DROP TABLE test_with_nans");
    }

    @Test
    public void testNestedTypes()
    {
        assertUpdate("CREATE TABLE test_nested_types (col1 INTEGER, col2 ROW (f1 INTEGER, f2 ARRAY(INTEGER), f3 DOUBLE))");
        assertUpdate("INSERT INTO test_nested_types VALUES " +
                "(7, ROW(3, ARRAY[10, 11, 19], 1.9)), " +
                "(-9, ROW(4, ARRAY[13, 16, 20], -2.9)), " +
                "(8, ROW(0, ARRAY[14, 17, 21], 3.9)), " +
                "(3, ROW(10, ARRAY[15, 18, 22], 4.9))", 4);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_nested_types$files\"");
        assertThat(materializedResult.getRowCount()).isEqualTo(1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        Map<Integer, String> lowerBounds = datafile.getLowerBounds();
        Map<Integer, String> upperBounds = datafile.getUpperBounds();

        // Only
        // 1. top-level primitive columns
        // 2. and nested primitive fields that are not descendants of LISTs or MAPs
        // should appear in lowerBounds or UpperBounds
        assertThat(lowerBounds.size()).isEqualTo(3);
        assertThat(upperBounds.size()).isEqualTo(3);

        // col1
        assertThat(lowerBounds.get(1)).isEqualTo("-9");
        assertThat(upperBounds.get(1)).isEqualTo("8");

        // col2.f1 (key in lowerBounds/upperBounds is Iceberg ID)
        assertThat(lowerBounds.get(3)).isEqualTo("0");
        assertThat(upperBounds.get(3)).isEqualTo("10");

        // col2.f3 (key in lowerBounds/upperBounds is Iceberg ID)
        assertThat(lowerBounds.get(5)).isEqualTo("-2.9");
        assertThat(upperBounds.get(5)).isEqualTo("4.9");

        assertUpdate("DROP TABLE test_nested_types");
    }

    @Test
    public void testWithTimestamps()
    {
        assertUpdate("CREATE TABLE test_timestamp (_timestamp TIMESTAMP(6)) WITH (format = 'ORC')");
        assertUpdate("INSERT INTO test_timestamp VALUES" +
                "(TIMESTAMP '2021-01-01 00:00:00.111111'), " +
                "(TIMESTAMP '2021-01-01 00:00:00.222222'), " +
                "(TIMESTAMP '2021-01-31 00:00:00.333333')", 3);
        MaterializedResult materializedResult = computeActual("SELECT * FROM \"test_timestamp$files\"");
        assertThat(materializedResult.getRowCount()).isEqualTo(1);
        DataFileRecord datafile = toDataFileRecord(materializedResult.getMaterializedRows().get(0));

        // Check file format
        assertThat(datafile.getFileFormat()).isEqualTo("ORC");

        // Check file row count
        assertThat(datafile.getRecordCount()).isEqualTo(3L);

        // Check per-column value count
        datafile.getValueCounts().values().forEach(valueCount -> assertThat(valueCount).isEqualTo((Long) 3L));

        // Check per-column null value count
        datafile.getNullValueCounts().values().forEach(nullValueCount -> assertThat(nullValueCount).isEqualTo((Long) 0L));

        // Check column lower bound. Min timestamp doesn't rely on file-level statistics and will not be truncated to milliseconds.
        assertThat(datafile.getLowerBounds().get(1)).isEqualTo("2021-01-01T00:00:00.111");
        assertQuery("SELECT min(_timestamp) FROM test_timestamp", "VALUES '2021-01-01 00:00:00.111111'");

        // Check column upper bound. Max timestamp doesn't rely on file-level statistics and will not be truncated to milliseconds.
        assertThat(datafile.getUpperBounds().get(1)).isEqualTo("2021-01-31T00:00:00.333999");
        assertQuery("SELECT max(_timestamp) FROM test_timestamp", "VALUES '2021-01-31 00:00:00.333333'");

        assertUpdate("DROP TABLE test_timestamp");
    }
}
